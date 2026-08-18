"""笔记工具：AI 讲到重点时调用 → 事件总线 + 落盘当日笔记文档。

「协同导读」的沉淀物：notes/YYYY-MM-DD.md 就是会话产出的文档。
M2 起 React UI 会把 ui.note 渲染成可寻址卡片；落盘文件是长期记忆。
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

NOTES_DIR = Path("notes")  # 项目内 notes/（裁剪归档也在这里）


def register(registry, ctx):
    @registry.register(
        "show_note",
        "仅限记录你刚刚亲口对用户说过的一句话结论（速记用途）。"
        "没口述过的内容、需要详细整理的内容，一律改用 request_note 让秘书写。"
        "content 要精炼，一两句话以内；有来源就附上 URL。",
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
        NOTES_DIR.mkdir(parents=True, exist_ok=True)
        path = NOTES_DIR / f"{datetime.now():%Y-%m-%d}.md"
        with path.open("a", encoding="utf-8") as f:
            f.write(f"- [{datetime.now():%H:%M}] **{title}** {content}\n")
        return {"ok": True, "note": title, "saved_to": str(path)}
