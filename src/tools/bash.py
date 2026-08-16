"""Bash 工具（对照 pi 的 bash.ts）：在工作区执行命令，截断输出。

语音特化：默认 30s 超时（pi 无默认超时——语音里不能无限等）。
"""

import asyncio
import tempfile

from src.tools._fs import ROOT

MAX_CHARS = 4000


def register(registry, ctx):
    @registry.register(
        "bash",
        "在用户电脑上执行 bash 命令（工作目录为主目录），返回 stdout+stderr（截断到末尾 4000 字符）。"
        "适合查文件、跑脚本、查系统信息。危险命令（删文件、改系统）执行前先跟用户确认。",        parameters={
            "type": "object",
            "properties": {
                "command": {"type": "string", "title": "要执行的 bash 命令"},
                "timeout": {"type": "number", "title": "超时秒数，默认 30"},
            },
            "required": ["command"],
        },
        timeout=45,
    )
    async def bash(command: str, timeout: int = 30) -> dict:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=ROOT,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return {"error": f"命令超过 {timeout} 秒未完成，已终止"}
        text = out.decode("utf-8", errors="replace")
        result = {"exit_code": proc.returncode, "output": text[-MAX_CHARS:]}
        if len(text) > MAX_CHARS:
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".log", prefix="bash_", delete=False)
            tmp.write(text)
            tmp.close()
            result["truncated"] = True
            result["full_output_path"] = tmp.name
        return result
