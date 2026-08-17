"""委派服务（Delegate）：把前台的"点单"路由给后台主力模型，完成后回注会话。

总线协议：
- delegate.think {question, context} → 异步跑主力模型（带 6 工具 agent 循环）
  → 完成后 inject "[深度思考结果]" + response.create → 前台主动播报
- delegate.note  {topic, points} → 秘书写卡（先上屏）
  → inject "[系统] 笔记已上屏" + response.create → 前台针对卡片讲解

attach(session)：每次会话重建（/model 切换、断线重连不影响——provider 引用跟随最新会话）。
防打断：前台正在说话（response 进行中）时先等它说完再注入，避免协议层 400。
"""

from __future__ import annotations

import asyncio
import logging

from .events import EventBus
from .secretary import Secretary
from .worker import SYSTEM, Worker

log = logging.getLogger("delegate")

THINK_PROMPT = """{system}

【最近的对话上下文】
{context}

【前台交办的任务】
{question}
{extra}
"""


class Delegate:
    def __init__(self, bus: EventBus):
        self.bus = bus
        self.worker = Worker(bus)
        self.secretary = Secretary(bus, self.worker)
        self._session = None          # 当前活跃 VoiceSession（attach 更新）
        self._tasks: set[asyncio.Task] = set()
        bus.subscribe(self._on_event)

    def attach(self, session):
        self._session = session

    def _on_event(self, evt):
        if evt.type == "delegate.think":
            self._spawn(self._do_think(evt.data.get("question", ""), evt.data.get("context", "")))
        elif evt.type == "delegate.note":
            self._spawn(self._do_note(evt.data.get("topic", ""), evt.data.get("points", "")))

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _notify_session(self, text: str, role: str = "user"):
        """注入 + 触发回复。等当前回复结束再注入，避免'推理进行中'协议错误。"""
        s = self._session
        if s is None:
            log.warning("无活跃会话，注入丢弃: %s", text[:60])
            return
        for _ in range(300):  # 最多等 30s
            if not s._response_active:
                break
            await asyncio.sleep(0.1)
        try:
            await s.provider.inject_text(text, role=role)
            await s.provider.create_response()
        except Exception as e:
            log.warning("注入失败（连接可能已断）: %s", e)

    async def _do_think(self, question: str, context: str):
        if not question:
            return
        try:
            prompt = THINK_PROMPT.format(
                system=SYSTEM,
                context=self.secretary.context_text(16),
                question=question,
                extra=f"\n【前台补充背景】{context}" if context else "")
            result = await self.worker.chat([{"role": "user", "content": prompt}])
            # 先上屏（完整 Markdown 原文），再让前台开口讲解——顺序即产品逻辑
            self.bus.publish("ui.worker", text=result)
            await self._notify_session(
                "[深度思考完成] 主力模型的详细结果已经显示在用户屏幕上。"
                "请看着它用一两句话向用户讲解要点（不要逐字念）：\n" + result[:2000])
        except Exception as e:
            log.warning("deep_think 失败: %s", e)
            self.bus.publish("tool.hint", text="主力模型暂时不可用")
            await self._notify_session(
                f"[深度思考失败] 后台主力模型暂时不可用（{e}），请向用户说明并用你自己的能力回答。")

    async def _do_note(self, topic: str, points: str):
        if not topic:
            return
        try:
            note = await self.secretary.make_note(topic, points)  # 卡片先上屏
            await self._notify_session(
                f"[系统] 笔记《{note['title']}》已上屏。请针对这张卡片用一两句话向用户讲解要点。",
                role="system")
        except Exception as e:
            log.warning("request_note 失败: %s", e)
            await self._notify_session(
                f"[系统] 笔记撰写失败（{e}），请直接向用户口述要点。", role="system")
