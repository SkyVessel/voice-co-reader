"""笔记工具：操控工具雏形（M2 实时笔记的胚胎）。

AI 讲到值得记录的重点时调用 → 往事件总线发 ui.note →
M1.5 CLI 打印出来；M2 起 React UI 渲染成可寻址卡片。
"""

import time


def register(registry, ctx):
    @registry.register(
        "show_note",
        "当你讲到一个值得用户记住的重点（结论、定义、关键数据）时调用，"
        "把它作为笔记展示给用户。content 要精炼，一两句话以内。",
        parameters={
            "type": "object",
            "properties": {
                "title": {"type": "string", "title": "笔记标题（几个字）"},
                "content": {"type": "string", "title": "笔记内容（一两句话）"},
            },
            "required": ["title", "content"],
        },
    )
    async def show_note(title: str, content: str, ctx) -> dict:
        ctx.bus.publish("ui.note", title=title, content=content, ts=time.time())
        return {"ok": True, "note": title}
