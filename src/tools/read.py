"""文件读取工具（对照 pi 的 read.ts）。范围见 _fs.py（默认用户主目录）。"""

from src.tools._fs import resolve

MAX_CHARS = 4000  # 语音场景：结果要能被口述摘要，截断比 pi 更狠


def register(registry, ctx):
    @registry.register(
        "read_file",
        "读取用户电脑上的文本文件（主目录内任意位置，如 Desktop、Documents）。大文件用 offset/limit 分段读。",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string", "title": "文件路径（相对主目录或绝对路径）"},
                "offset": {"type": "number", "title": "起始行号（从 1 开始）"},
                "limit": {"type": "number", "title": "最多读取行数"},
            },
            "required": ["path"],
        },
    )
    async def read_file(path: str, offset: int = 1, limit: int = 200) -> dict:
        p = resolve(path)
        if not p.is_file():
            return {"error": f"文件不存在：{path}"}
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        total = len(lines)
        chunk = lines[offset - 1: offset - 1 + limit]
        text = "\n".join(chunk)
        truncated = False
        if len(text) > MAX_CHARS:
            text = text[:MAX_CHARS]
            truncated = True
        return {
            "content": text,
            "total_lines": total,
            "showing": f"{offset}~{offset + len(chunk) - 1}",
            "truncated": truncated,
        }
