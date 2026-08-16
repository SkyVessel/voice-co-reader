"""工具层：ToolRegistry + Dispatcher。

仅聊天版和未来工具版的核心差异 = 注册表是空的还是满的。
- 取数工具（搜索/看图）：handler 调外部 API，结果回传模型口述
- 操控工具（show_card/highlight）：handler 往事件总线发 UI 事件，回传 "ok"

M1.5 起，工具定义为 tools/*.py 文件 + 文件监视器热加载（pi 式 reload）。
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any, Awaitable, Callable

log = logging.getLogger("tools")

ToolHandler = Callable[..., Awaitable[Any]]


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, tuple[dict, ToolHandler]] = {}

    def register(self, name: str, description: str,
                 parameters: dict | None = None):
        """装饰器：注册一个 Function Calling 工具。"""
        def deco(fn: ToolHandler):
            self._tools[name] = ({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters or {"type": "object", "properties": {}},
                },
            }, fn)
            log.info("tool registered: %s", name)
            return fn
        return deco

    def schemas(self) -> list[dict]:
        """注入 session.update 的 tools 字段。"""
        return [schema for schema, _ in self._tools.values()]

    async def dispatch(self, name: str, arguments_json: str) -> str:
        """执行工具，返回 JSON 字符串结果（写回 function_call_output）。"""
        entry = self._tools.get(name)
        if entry is None:
            return json.dumps({"error": f"unknown tool: {name}"})
        _, handler = entry
        try:
            args = json.loads(arguments_json or "{}")
            result = handler(**args)
            if inspect.isawaitable(result):
                result = await result
            return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
        except Exception as e:
            log.exception("tool %s failed", name)
            return json.dumps({"error": str(e)}, ensure_ascii=False)
