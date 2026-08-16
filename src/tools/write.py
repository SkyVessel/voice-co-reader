"""文件写入工具（对照 pi 的 write.ts）：整文件创建/覆盖。限定工作区。"""

from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent.parent


def _resolve(path: str) -> Path:
    p = (WORKSPACE / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
    if not str(p).startswith(str(WORKSPACE)):
        raise ValueError(f"路径越界：{path}（只允许工作区内文件）")
    return p


def register(registry, ctx):
    @registry.register(
        "write_file",
        "创建或完整覆盖工作区内的一个文件。适用于保存笔记、调研结果、新建小文件。",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string", "title": "文件路径（相对项目根目录）"},
                "content": {"type": "string", "title": "完整文件内容"},
            },
            "required": ["path", "content"],
        },
    )
    async def write_file(path: str, content: str) -> dict:
        p = _resolve(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return {"ok": True, "path": str(p.relative_to(WORKSPACE)), "chars": len(content)}
