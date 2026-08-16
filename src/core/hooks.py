"""Hooks：会话生命周期钩子，pi 式扩展点。

on_turn_start    用户开始说话（一轮开始）
on_interrupt     用户打断了 AI 播报
on_tool_call     模型请求调用工具（name/arguments）
on_tool_result   工具执行完毕（name/result）
on_turn_end      一轮结束（response.done，含 status）
on_reload        工具/技能热重载完成（tools/skills）

扩展（extensions/）注册钩子函数；同步异步均可。
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable

log = logging.getLogger("hooks")

HOOK_NAMES = (
    "on_turn_start", "on_interrupt", "on_tool_call",
    "on_tool_result", "on_turn_end", "on_reload",
)


class Hooks:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable]] = {n: [] for n in HOOK_NAMES}

    def on(self, name: str, fn: Callable) -> None:
        if name not in self._handlers:
            raise ValueError(f"未知钩子: {name}（可选: {HOOK_NAMES}）")
        self._handlers[name].append(fn)

    async def emit(self, hook: str, **data: Any) -> None:
        for fn in self._handlers.get(hook, []):
            try:
                r = fn(**data)
                if inspect.isawaitable(r):
                    await r
            except Exception:
                log.exception("hook %s 处理失败", hook)
