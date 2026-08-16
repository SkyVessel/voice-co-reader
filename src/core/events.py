"""事件总线：核心发生的一切都是事件，UI 和扩展只是订阅者。

设计来源：pi 的 hooks/RPC 分层思想。M1 进程内同步派发；
M2 起 UI 变成 WebSocket 订阅同一事件流的远程客户端。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

log = logging.getLogger("bus")


@dataclass
class Event:
    type: str
    data: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)


Subscriber = Callable[[Event], None]


class EventBus:
    def __init__(self) -> None:
        self._subs: list[Subscriber] = []
        self.history: list[Event] = []  # 会话审计/回放用（M3 索引回溯的基础）

    def subscribe(self, fn: Subscriber) -> None:
        self._subs.append(fn)

    def publish(self, type: str, **data: Any) -> None:
        evt = Event(type=type, data=data)
        self.history.append(evt)
        log.debug("event %s %s", type, {k: v for k, v in data.items() if k != "pcm"})
        for fn in self._subs:
            try:
                fn(evt)
            except Exception:
                log.exception("subscriber error on %s", type)
