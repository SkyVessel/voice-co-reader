"""随航秘书：订阅对话总线，攒上下文，让主力模型代写笔记。

设计动机（协同导读的关键体验）：
- realtime（前台）被提示词要求"说话简短"，自己写笔记必然简陋 → 写作交给主力模型
- 卡片必须先上屏，前台收到通知后再针对卡片讲解 → 顺序由本组件流程焊死
- 上下文共享：秘书直接读对话原文（总线转写），不靠前台转述

产出格式与 show_note 一致（- [HH:MM] **标题** 内容 → notes/YYYY-MM-DD.md），
因此 UI 回填（bridge.read_today_notes）无需改动。
"""

from __future__ import annotations

import logging
import re
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from .events import EventBus
from .worker import Worker

log = logging.getLogger("secretary")

NOTES_DIR = Path("notes")

NOTE_PROMPT = """你是语音对话的随航秘书。根据下面的对话原文，为指定主题写一张笔记卡片。

要求：
- 只写对话中真实出现的信息，不许编造；有来源链接就附上
- 内容详实（100~250 字）：关键数据、结论、出处都要留住，宁可全不要省
- 标题≤12 字，一眼能认出主题
- 输出格式严格两行：
标题：xxx
正文：xxx

【主题】{topic}
【前台标注的要点】{points}
【对话原文】
{context}
"""


class Secretary:
    def __init__(self, bus: EventBus, worker: Worker):
        self.bus = bus
        self.worker = worker
        self._buf: deque[str] = deque(maxlen=60)  # 最近对话原文（"谁: 内容"）
        bus.subscribe(self._on_event)

    def _on_event(self, evt):
        if evt.type == "user.transcript_done":
            text = evt.data.get("transcript", "").strip()
            if text:
                self._buf.append(f"用户: {text}")
        elif evt.type == "user.typed":  # 键盘输入也算对话原文
            text = evt.data.get("text", "").strip()
            if text:
                self._buf.append(f"用户: {text}")
        elif evt.type == "assistant.transcript_done":
            text = evt.data.get("transcript", "").strip()
            if text:
                self._buf.append(f"前台: {text}")

    def context_text(self, n: int = 16) -> str:
        """最近 n 条对话原文，供主力模型共享上下文。"""
        items = list(self._buf)[-n:]
        return "\n".join(items) if items else "（暂无对话记录）"

    async def make_note(self, topic: str, points: str) -> dict:
        """点单 → 主力模型写卡 → 上屏 + 落盘。返回 {title, content}。"""
        if not self.worker.available:
            # 无主力模型时的兜底：用前台的要点直接成卡（不退化体验）
            content = points or topic
            title = topic[:12]
        else:
            prompt = NOTE_PROMPT.format(topic=topic, points=points or "（无）",
                                        context=self.context_text(24))
            raw = await self.worker.chat(
                [{"role": "user", "content": prompt}], with_tools=False, max_tokens=1200)
            m = re.search(r"标题[：:]\s*(.+?)\s*\n正文[：:]\s*(.+)", raw, re.S)
            if m:
                title, content = m.group(1).strip()[:24], m.group(2).strip()
            else:  # 格式没遵守：全文当内容，主题当标题
                log.warning("笔记格式异常，原文兜底: %s", raw[:80])
                title, content = topic[:12], raw.strip()
        self._publish(title, content)
        return {"title": title, "content": content}

    def _publish(self, title: str, content: str):
        self.bus.publish("ui.note", title=title, content=content, ts=time.time())
        NOTES_DIR.mkdir(parents=True, exist_ok=True)
        path = NOTES_DIR / f"{datetime.now():%Y-%m-%d}.md"
        with path.open("a", encoding="utf-8") as f:
            f.write(f"- [{datetime.now():%H:%M}] **{title}** {content}\n")
        log.info("笔记已生成: %s", title)
