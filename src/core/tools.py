"""工具层：ToolRegistry + Dispatcher + 目录加载。

仅聊天版和未来工具版的核心差异 = 注册表是空的还是满的。
- 取数工具（搜索/看图）：handler 调外部 API，结果回传模型口述
- 操控工具（show_note/highlight）：handler 往事件总线发 UI 事件，回传 "ok"

工具定义为 src/tools/*.py 文件（每文件可含多个工具），由 reload.py 热加载。
约定：模块提供 register(registry, ctx) 函数。
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .events import EventBus

log = logging.getLogger("tools")

DEFAULT_TOOL_TIMEOUT = 8.0  # 延迟预算：工具不得让对话冷场超过 8s


@dataclass
class ToolContext:
    """注入工具 handler 的上下文（对照 pi 的 tool-context）。"""
    bus: "EventBus"


ToolHandler = Callable[..., Awaitable[Any]]


class ToolRegistry:
    def __init__(self, ctx: ToolContext) -> None:
        self.ctx = ctx
        self._tools: dict[str, tuple[dict, ToolHandler, float]] = {}

    def register(self, name: str, description: str,
                 parameters: dict | None = None,
                 timeout: float = DEFAULT_TOOL_TIMEOUT):
        """装饰器：注册一个 Function Calling 工具。"""
        def deco(fn: ToolHandler):
            self._tools[name] = ({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters or {"type": "object", "properties": {}},
                },
            }, fn, timeout)
            log.info("tool registered: %s", name)
            return fn
        return deco

    def clear(self):
        self._tools.clear()

    def schemas(self) -> list[dict]:
        """注入 session.update 的 tools 字段。"""
        return [schema for schema, _, _ in self._tools.values()]

    def names(self) -> list[str]:
        return list(self._tools)

    async def dispatch(self, name: str, arguments_json: str) -> str:
        """执行工具，返回 JSON 字符串（写回 function_call_output）。

        错误哲学（pi result.ts + 语音特化）：失败不抛异常，
        返回可口述的结构化错误——模型会把它说给用户听。
        """
        entry = self._tools.get(name)
        if entry is None:
            return json.dumps({"error": f"工具 {name} 不存在"}, ensure_ascii=False)
        _, handler, timeout = entry
        try:
            args = json.loads(arguments_json or "{}")
            kwargs = dict(args)
            if "ctx" in inspect.signature(handler).parameters:
                kwargs["ctx"] = self.ctx
            result = await asyncio.wait_for(_maybe_await(handler(**kwargs)), timeout=timeout)
            return result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
        except asyncio.TimeoutError:
            log.warning("tool %s timeout (%.0fs)", name, timeout)
            return json.dumps({"error": f"工具执行超过 {timeout:.0f} 秒，超时了"}, ensure_ascii=False)
        except Exception as e:
            log.exception("tool %s failed", name)
            return json.dumps({"error": f"工具执行失败：{e}"}, ensure_ascii=False)


async def _maybe_await(v):
    return await v if inspect.isawaitable(v) else v


def load_tools_from_dir(registry: ToolRegistry, tools_dir: str | Path) -> list[str]:
    """把目录下每个 .py 文件作为工具模块加载（fresh import，天然支持热重载语义）。"""
    loaded: list[str] = []
    d = Path(tools_dir)
    if not d.is_dir():
        return loaded
    for path in sorted(d.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            spec = importlib.util.spec_from_file_location(f"voice_tool_{path.stem}", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.register(registry, registry.ctx)
            loaded.append(path.stem)
        except Exception:
            log.exception("加载工具模块失败: %s", path)
    return loaded
