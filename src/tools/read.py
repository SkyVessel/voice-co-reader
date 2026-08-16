"""文件读取工具（对照 pi 的 read.ts）。

安全约束：路径限定在项目工作区内，禁止逃逸。
"""

from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent.parent  # 项目根目录
MAX_CHARS = 4000  # 语音场景：结果要能被口述摘要，截断比 pi 更狠


def _resolve(path: str) -> Path:
    p = (WORKSPACE / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
    if not str(p).startswith(str(WORKSPACE)):
        raise ValueError(f"路径越界：{path}（只允许工作区内文件）")
    return p


def register(registry, ctx):
    @registry.register(
        "read_file",
        "读取工作区内文本文件的内容。大文件用 offset/limit 分段读。",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string", "title": "文件路径（相对项目根目录）"},
                "offset": {"type": "number", "title": "起始行号（从 1 开始）"},
                "limit": {"type": "number", "title": "最多读取行数"},
            },
            "required": ["path"],
        },
    )
    async def read_file(path: str, offset: int = 1, limit: int = 200) -> dict:
        p = _resolve(path)
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
