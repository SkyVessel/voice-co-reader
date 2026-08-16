"""文件写入工具（对照 pi 的 write.ts）：整文件创建/覆盖。范围见 _fs.py。"""

from src.tools._fs import ROOT, resolve


def register(registry, ctx):
    @registry.register(
        "write_file",
        "在用户电脑上创建或完整覆盖一个文件（主目录内任意位置）。适用于保存笔记、调研结果、新建文件。",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string", "title": "文件路径（相对主目录或绝对路径）"},
                "content": {"type": "string", "title": "完整文件内容"},
            },
            "required": ["path", "content"],
        },
    )
    async def write_file(path: str, content: str) -> dict:
        p = resolve(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return {"ok": True, "path": str(p.relative_to(ROOT)), "chars": len(content)}
