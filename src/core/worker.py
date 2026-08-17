"""主力模型客户端（"后台大脑"）：OpenAI 兼容 chat completions + 工具 agent 循环。

角色定位：realtime 是前台接待，这里住的是干活的主力（默认 deepseek-v4-flash-free）。
- vendor-neutral：任何 OpenAI 兼容端点，改 .env 即换（TEXT_MODEL_BASE/TEXT_MODEL/OPENCODE_API_KEY）
- 工具循环：主力模型自带 6 个工具（read/write/edit/bash/web_search/web_fetch），可多轮迭代
- 容错：429/5xx 指数退避重试（免费模型常被限流）；失败抛 RuntimeError 由调用方兜底
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.request

from .events import EventBus
from .tools import ToolContext, ToolRegistry, load_tools_from_dir

log = logging.getLogger("worker")

WORKER_TOOLS = ["read", "write", "edit", "bash", "web_search", "web_fetch"]
MAX_ROUNDS = 6          # 工具循环上限（防失控）
RETRY_DELAYS = [6, 18, 45]  # 429/5xx 退避秒数
HTTP_TIMEOUT = 90

SYSTEM = (
    "你是一个语音助手背后的主力模型。用户通过语音前台（另一个模型）与你间接交流。"
    "认真完成交办的任务，需要时调用工具查证或操作。"
    "最终回答用中文，精炼、结构清楚；若结果会被口述，控制在 300 字以内。"
)


class Worker:
    def __init__(self, bus: EventBus, tools_dir: str = "src/tools"):
        self.bus = bus
        self.base = os.environ.get("TEXT_MODEL_BASE", "https://opencode.ai/zen/v1")
        self.model = os.environ.get("TEXT_MODEL", "deepseek-v4-flash-free")
        self.key = os.environ.get("OPENCODE_API_KEY", "")
        # 主力模型的私有工具箱（与 realtime 前台共享同一套工具模块，独立注册表）
        self.registry = ToolRegistry(ToolContext(bus))
        for path in sorted(__import__("pathlib").Path(tools_dir).glob("*.py")):
            if path.stem in WORKER_TOOLS:
                import importlib.util
                spec = importlib.util.spec_from_file_location(f"worker_tool_{path.stem}", path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                module.register(self.registry, self.registry.ctx)
        log.info("worker 工具箱: %s", self.registry.names())

    @property
    def available(self) -> bool:
        return bool(self.key)

    def _post(self, payload: dict) -> dict:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{self.base}/chat/completions", data=body,
            headers={"Authorization": f"Bearer {self.key}", "Content-Type": "application/json",
                     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})  # 无 UA 会被网关 403
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read())

    async def _post_retry(self, payload: dict) -> dict:
        last = None
        for attempt in range(len(RETRY_DELAYS) + 1):
            try:
                return await asyncio.to_thread(self._post, payload)
            except urllib.error.HTTPError as e:
                last = e
                if e.code in (429, 500, 502, 503, 504) and attempt < len(RETRY_DELAYS):
                    wait = RETRY_DELAYS[attempt]
                    log.warning("worker HTTP %s，%ds 后重试", e.code, wait)
                    self.bus.publish("worker.retry", wait=wait, code=e.code)
                    await asyncio.sleep(wait)
                    continue
                raise RuntimeError(f"主力模型 HTTP {e.code}") from e
            except Exception as e:
                raise RuntimeError(f"主力模型调用失败: {e}") from e
        raise RuntimeError(f"主力模型重试耗尽: {last}")

    async def chat(self, messages: list[dict], with_tools: bool = True,
                   max_tokens: int | None = None) -> str:
        """agent 循环：工具调用往返直至模型给出最终文本。"""
        if not self.available:
            raise RuntimeError("未配置 OPENCODE_API_KEY")
        payload: dict = {"model": self.model, "messages": messages}
        if with_tools:
            payload["tools"] = self.registry.schemas()
        if max_tokens:
            payload["max_tokens"] = max_tokens
        for _ in range(MAX_ROUNDS):
            r = await self._post_retry(payload)
            msg = r["choices"][0]["message"]
            calls = msg.get("tool_calls") or []
            if not calls:
                return (msg.get("content") or "").strip()
            payload["messages"].append({"role": "assistant", "content": msg.get("content"),
                                        "tool_calls": calls})
            for c in calls:  # 串行执行（写类工具不乱序）
                name = c["function"]["name"]
                args = c["function"].get("arguments") or "{}"
                self.bus.publish("worker.tool", name=name, arguments=args)
                result = await self.registry.dispatch(name, args)
                payload["messages"].append(
                    {"role": "tool", "tool_call_id": c["id"], "content": result[:12000]})
        return "（工具轮次耗尽，任务未完成）"
