"""Provider 层：RealtimeProvider 抽象 + Qwen Audio Realtime (WebSocket) 实现。

所有协议事件在这里被规范化为内部事件名（user.speech_started 等），
核心与 UI 永远不直接接触 Qwen 字段——换 Provider（Grok/Gemini）时只改本文件。

协议字段依据：阿里云百炼 Qwen-Audio Realtime WebSocket API 参考（2026-08 核读），
不要凭记忆改字段名，改前先查 docs/调研-语音模型选型.md 里的官方文档链接。
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator

import websockets

log = logging.getLogger("provider")

# Qwen 事件类型 → 内部事件名
_EVENT_MAP = {
    "session.created": "session.created",
    "session.updated": "session.updated",
    "input_audio_buffer.speech_started": "user.speech_started",
    "input_audio_buffer.speech_stopped": "user.speech_stopped",
    "input_audio_buffer.committed": "user.committed",
    "conversation.item.created": "item.created",
    "conversation.item.input_audio_transcription.delta": "user.transcript_delta",
    "conversation.item.input_audio_transcription.completed": "user.transcript_done",
    "response.created": "response.created",
    "response.audio_transcript.delta": "assistant.transcript_delta",
    "response.audio_transcript.done": "assistant.transcript_done",
    "response.audio.delta": "assistant.audio_delta",
    "response.audio.done": "assistant.audio_done",
    "response.function_call_arguments.done": "tool.call",
    "response.done": "response.done",
    "error": "error",
}


class RealtimeProvider(ABC):
    """语音模型 Provider 接口：五个方法封顶（对齐 OpenAI Realtime 风格）。"""

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def update_session(self, config: dict[str, Any]) -> None: ...

    @abstractmethod
    async def send_audio(self, pcm: bytes) -> None: ...

    @abstractmethod
    async def write_tool_result(self, call_id: str, output: str) -> None: ...

    @abstractmethod
    async def delete_item(self, item_id: str) -> None: ...  # 主动裁剪：删历史 item

    @abstractmethod
    async def events(self) -> AsyncIterator[dict[str, Any]]:
        """持续产出规范化事件：{"type": ..., ...payload}"""

    async def close(self) -> None: ...


class QwenRealtimeProvider(RealtimeProvider):
    def __init__(self, api_key: str, model: str,
                 ws_base: str = "wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime"):
        self.api_key = api_key
        self.model = model
        self.url = f"{ws_base}?model={model}"
        self._ws = None

    async def connect(self) -> None:
        self._ws = await websockets.connect(
            self.url,
            additional_headers={"Authorization": f"Bearer {self.api_key}"},
            open_timeout=15, ping_interval=20, ping_timeout=20,
        )
        log.info("connected: %s", self.url)

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._ws is None or self._ws.state.name != "OPEN":
            raise ConnectionError(
                f"WebSocket 未连接或已断开 (state={self._ws.state.name if self._ws is not None else None})")
        await self._ws.send(json.dumps(payload))

    async def update_session(self, config: dict[str, Any]) -> None:
        await self._send({"type": "session.update", "session": config})

    async def send_audio(self, pcm: bytes) -> None:
        await self._send({
            "type": "input_audio_buffer.append",
            "audio": base64.b64encode(pcm).decode(),
        })

    async def write_tool_result(self, call_id: str, output: str) -> None:
        """写回工具结果。注意：之后不能立刻 response.create，
        要等当前 response.done（协议：推理进行中不允许 create）。"""
        await self._send({
            "type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": call_id, "output": output},
        })

    async def delete_item(self, item_id: str) -> None:
        await self._send({"type": "conversation.item.delete", "item_id": item_id})

    async def inject_text(self, text: str, role: str = "user") -> None:
        """注入文本消息（文本降级模式 / 冒烟测试用）。"""
        await self._send({
            "type": "conversation.item.create",
            "item": {"type": "message", "role": role,
                     "content": [{"type": "input_text", "text": text}]},
        })

    async def create_response(self) -> None:
        await self._send({"type": "response.create"})

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        async for raw in self._ws:
            msg = json.loads(raw)
            qtype = msg.get("type", "")
            mapped = _EVENT_MAP.get(qtype)
            if mapped is None:
                log.debug("unmapped event: %s", qtype)
                continue
            evt: dict[str, Any] = {"type": mapped, "raw_type": qtype}
            if qtype == "response.audio.delta":
                evt["pcm"] = base64.b64decode(msg.get("delta", ""))
            elif qtype in ("response.audio_transcript.delta",
                           "conversation.item.input_audio_transcription.delta"):
                evt["delta"] = msg.get("delta") or msg.get("text") or ""
                evt["stash"] = msg.get("stash", "")
            elif qtype == "conversation.item.input_audio_transcription.completed":
                evt["transcript"] = msg.get("transcript", "")
                evt["item_id"] = msg.get("item_id")
            elif qtype == "response.audio_transcript.done":
                evt["transcript"] = msg.get("transcript", "")
                evt["item_id"] = msg.get("item_id")
            elif qtype == "response.function_call_arguments.done":
                evt.update(call_id=msg.get("call_id"), name=msg.get("name"),
                           arguments=msg.get("arguments", "{}"),
                           item_id=msg.get("item_id"))
            elif qtype == "input_audio_buffer.committed":
                evt["item_id"] = msg.get("item_id")
            elif qtype == "conversation.item.created":
                evt["item"] = msg.get("item", {})
            elif qtype == "response.done":
                resp = msg.get("response", {})
                evt["status"] = resp.get("status")
                evt["reason"] = (resp.get("status_details") or {}).get("reason")
                evt["usage"] = resp.get("usage")  # token 用量（裁剪效果监控 + 商业化计量）
            elif qtype == "error":
                evt["error"] = msg.get("error", {})
            elif qtype in ("session.created", "session.updated"):
                evt["session"] = msg.get("session", {})
            elif qtype == "input_audio_buffer.speech_stopped":
                evt["reason"] = msg.get("reason")  # smart_turn: turn_invalid
            yield evt

    async def close(self) -> None:
        if self._ws:
            await self._ws.close()


# ─── OpenAI Realtime (GA) ─────────────────────────────────────────────
# 协议依据：OpenAI Realtime API GA 文档（2026-08 核读 developers.openai.com）
# 与 Qwen 的差异：音频一律 24kHz（输入需重采样）；session 字段组织在 audio.input/output 下；
# 工具调用经 response.output_item.done（item.type==function_call）而非 function_call_arguments.done。

_OA_EVENT_MAP = {
    "session.created": "session.created",
    "session.updated": "session.updated",
    "input_audio_buffer.speech_started": "user.speech_started",
    "input_audio_buffer.speech_stopped": "user.speech_stopped",
    "input_audio_buffer.committed": "user.committed",
    "conversation.item.created": "item.created",
    "conversation.item.input_audio_transcription.delta": "user.transcript_delta",
    "conversation.item.input_audio_transcription.completed": "user.transcript_done",
    "response.created": "response.created",
    # 事件名两族并存：经典扁平 schema（GA 2025，中转站透传）用 response.audio.*，
    # 统一接口（2026 新）用 response.output_audio.* —— 都映射到同一内部事件
    "response.audio.delta": "assistant.audio_delta",
    "response.output_audio.delta": "assistant.audio_delta",
    "response.audio_transcript.delta": "assistant.transcript_delta",
    "response.output_audio_transcript.delta": "assistant.transcript_delta",
    "response.done": "response.done",
    "error": "error",
}

_OA_VOICES = {"alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse", "marin", "cedar"}


def _resample_16k_to_24k(pcm: bytes) -> bytes:
    """线性插值重采样（16k→24k，3:2）。语音带宽低，线性插值足够。"""
    import numpy as np
    x = np.frombuffer(pcm, dtype=np.int16)
    if len(x) == 0:
        return b""
    n_out = len(x) * 3 // 2
    xi = np.linspace(0, len(x) - 1, n_out)
    y = np.interp(xi, np.arange(len(x)), x.astype(np.float32))
    return y.astype(np.int16).tobytes()


class OpenAIRealtimeProvider(RealtimeProvider):
    def __init__(self, api_key: str, model: str = "gpt-realtime-2.1",
                 ws_base: str = "wss://api.openai.com/v1/realtime"):
        self.api_key = api_key
        self.model = model
        self.url = f"{ws_base}?model={model}"
        self._ws = None

    async def connect(self) -> None:
        self._ws = await websockets.connect(
            self.url,
            additional_headers={"Authorization": f"Bearer {self.api_key}"},
            open_timeout=15, ping_interval=20, ping_timeout=20,
        )
        log.info("connected: %s", self.url)

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._ws is None or self._ws.state.name != "OPEN":
            raise ConnectionError(
                f"WebSocket 未连接或已断开 (state={self._ws.state.name if self._ws is not None else None})")
        if os.environ.get("DEBUG_EVENTS"):
            with open("/tmp/oa_events.log", "a") as f:
                f.write(f">> {json.dumps(payload, ensure_ascii=False)[:300]}\n")
        await self._ws.send(json.dumps(payload))

    async def update_session(self, config: dict[str, Any]) -> None:
        """把通用 config 翻译成 OpenAI 扁平 GA session 结构。
        实测（2026-08，经 CometAPI 中转 gpt-realtime-2.1-mini）：统一接口的嵌套结构
        （session.type=realtime + audio.input/output）会被拒绝 unknown_parameter，
        经典扁平 schema 可用。"""
        voice = config.get("voice", "alloy")
        if voice not in _OA_VOICES:
            voice = "alloy"
        session: dict[str, Any] = {
            "modalities": ["audio", "text"],
            "instructions": config.get("instructions", ""),
            "voice": voice,
            "input_audio_format": "pcm16",
            "output_audio_format": "pcm16",
            "input_audio_transcription": {"model": "gpt-4o-mini-transcribe"},
        }
        td = config.get("turn_detection")
        if td is not None:
            session["turn_detection"] = td  # server_vad 参数名与 Qwen 一致
        tools = config.get("tools")
        if tools:
            # 我们的 schema 是 HTTP 嵌套风格 {type, function:{...}}；Realtime 要扁平的
            session["tools"] = [
                {"type": "function", **t["function"]} if "function" in t else {"type": "function", **t}
                for t in tools
            ]
        await self._send({"type": "session.update", "session": session})

    async def send_audio(self, pcm: bytes) -> None:
        # OpenAI 要求 24kHz 输入，本机麦克风采 16kHz → 重采样
        await self._send({
            "type": "input_audio_buffer.append",
            "audio": base64.b64encode(_resample_16k_to_24k(pcm)).decode(),
        })

    async def write_tool_result(self, call_id: str, output: str) -> None:
        await self._send({
            "type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": call_id, "output": output},
        })

    async def delete_item(self, item_id: str) -> None:
        await self._send({"type": "conversation.item.delete", "item_id": item_id})

    async def inject_text(self, text: str, role: str = "user") -> None:
        await self._send({
            "type": "conversation.item.create",
            "item": {"type": "message", "role": role,
                     "content": [{"type": "input_text", "text": text}]},
        })

    async def create_response(self) -> None:
        await self._send({"type": "response.create"})

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        seen_tool_calls: set = set()
        async for raw in self._ws:
            msg = json.loads(raw)
            if os.environ.get("DEBUG_EVENTS"):
                t = msg.get("type", "")
                if not t.endswith("audio.delta"):
                    with open("/tmp/oa_events.log", "a") as f:
                        f.write(f"<< {json.dumps(msg, ensure_ascii=False)[:300]}\n")
            otype = msg.get("type", "")
            # 工具调用两族并存：经典 schema 用 response.function_call_arguments.done，
            # 统一接口用 response.output_item.done（item.type==function_call）。
            # 坑：经典 API 同一调用两种事件都会发——必须按 call_id 去重，只报一次！
            if otype == "response.function_call_arguments.done":
                cid = msg.get("call_id")
                if cid not in seen_tool_calls:
                    seen_tool_calls.add(cid)
                    yield {"type": "tool.call", "raw_type": otype,
                           "call_id": cid, "name": msg.get("name"),
                           "arguments": msg.get("arguments", "{}"),
                           "item_id": msg.get("item_id")}
                continue
            if otype == "response.output_item.done":
                item = msg.get("item", {})
                if item.get("type") == "function_call":
                    cid = item.get("call_id")
                    if cid not in seen_tool_calls:
                        seen_tool_calls.add(cid)
                        yield {"type": "tool.call", "raw_type": otype,
                               "call_id": cid, "name": item.get("name"),
                               "arguments": item.get("arguments", "{}"),
                               "item_id": item.get("id")}
                else:
                    yield {"type": "item.done", "raw_type": otype, "item": item}
                continue
            mapped = _OA_EVENT_MAP.get(otype)
            if mapped is None:
                log.debug("unmapped event: %s", otype)
                continue
            evt: dict[str, Any] = {"type": mapped, "raw_type": otype}
            if otype in ("response.audio.delta", "response.output_audio.delta"):
                evt["pcm"] = base64.b64decode(msg.get("delta", ""))
            elif otype in ("response.audio_transcript.delta",
                           "response.output_audio_transcript.delta",
                           "conversation.item.input_audio_transcription.delta"):
                evt["delta"] = msg.get("delta", "")
                evt["stash"] = ""
            elif otype == "conversation.item.input_audio_transcription.completed":
                evt["transcript"] = msg.get("transcript", "")
                evt["item_id"] = msg.get("item_id")
            elif otype == "input_audio_buffer.committed":
                evt["item_id"] = msg.get("item_id")
            elif otype == "conversation.item.created":
                evt["item"] = msg.get("item", {})
            elif otype == "response.done":
                resp = msg.get("response", {})
                evt["status"] = resp.get("status")
                evt["reason"] = (resp.get("status_details") or {}).get("reason")
                evt["usage"] = resp.get("usage")
            elif otype == "error":
                evt["error"] = msg.get("error", {})
            elif otype in ("session.created", "session.updated"):
                evt["session"] = msg.get("session", {})
            yield evt

    async def close(self) -> None:
        if self._ws:
            await self._ws.close()


PROVIDERS = {
    "qwen": lambda key, model, ws_base="": QwenRealtimeProvider(key, model or "qwen-audio-3.0-realtime-plus"),
    "openai": lambda key, model, ws_base="": OpenAIRealtimeProvider(
        key, model or "gpt-realtime-2.1",
        **({"ws_base": ws_base} if ws_base else {})),
}


def create_provider(name: str, api_key: str, model: str = "", ws_base: str = "") -> RealtimeProvider:
    """按名字创建 Provider（Vendor-Neutral 的落地）。ws_base 用于中转站/自建网关。"""
    if name not in PROVIDERS:
        raise ValueError(f"未知 provider: {name}（可选: {list(PROVIDERS)}）")
    return PROVIDERS[name](api_key, model, ws_base)
