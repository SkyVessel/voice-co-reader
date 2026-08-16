"""文件编辑工具（对照 pi 的 edit.ts）：精确文本替换。范围见 _fs.py。"""

from src.tools._fs import ROOT, resolve


def register(registry, ctx):
    @registry.register(
        "edit_file",
        "对文件做精确文本替换：把 old_text 替换为 new_text。"
        "old_text 必须在文件中唯一出现。适合小改动；大改用 write_file。",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string", "title": "文件路径（相对主目录或绝对路径）"},
                "old_text": {"type": "string", "title": "要被替换的原文（需唯一匹配）"},
                "new_text": {"type": "string", "title": "替换后的新文本"},
            },
            "required": ["path", "old_text", "new_text"],
        },
    )
    async def edit_file(path: str, old_text: str, new_text: str) -> dict:
        p = resolve(path)
        if not p.is_file():
            return {"error": f"文件不存在：{path}"}
        text = p.read_text(encoding="utf-8")
        count = text.count(old_text)
        if count == 0:
            return {"error": "old_text 未找到，未做修改"}
        if count > 1:
            return {"error": f"old_text 出现 {count} 次，不唯一，未做修改。请提供更长的上下文。"}
        p.write_text(text.replace(old_text, new_text, 1), encoding="utf-8")
        return {"ok": True, "path": str(p.relative_to(ROOT))}
