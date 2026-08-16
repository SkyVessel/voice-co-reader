"""音色试听：生成固定短句的音频（PCM16 24kHz），磁盘缓存后零成本。

- OpenAI：走 /audio/speech TTS（音色名与 realtime 一致），不碰 realtime token
- Qwen：TTS 音色与 realtime 音色是两套体系 → 用一次性 realtime 会话生成（仅首次，
  一句话约 ¥0.02），缓存后永久免费。用户要求"不消耗模型 token"→ 缓存是兑现方式。
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.request
from pathlib import Path

log = logging.getLogger("tts")

PREVIEW_TEXT = "你好呀，我是你的语音助手，这是你当前选择的音色。"
CACHE_DIR = Path(".cache/voice_previews")


def _cache_path(provider: str, voice: str) -> Path:
    return CACHE_DIR / f"{provider}_{voice}.pcm"


async def voice_preview(provider: str, voice: str, api_key: str,
                        ws_base: str = "", model: str = "") -> bytes | None:
    """返回 PCM16 24kHz 音频；失败返回 None。命中缓存直接读盘。"""
    cache = _cache_path(provider, voice)
    if cache.is_file():
        return cache.read_bytes()
    try:
        if provider == "openai":
            pcm = await asyncio.to_thread(_openai_tts, voice, api_key, ws_base)
        elif provider == "qwen":
            pcm = await _qwen_preview(voice, api_key, model or "qwen-audio-3.0-realtime-flash")
        else:
            return None
    except Exception:
        log.exception("音色试听生成失败: %s/%s", provider, voice)
        return None
    if pcm:
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(pcm)
    return pcm or None


def _openai_tts(voice: str, api_key: str, ws_base: str) -> bytes:
    """OpenAI 兼容 TTS：POST /audio/speech，response_format=pcm（24kHz PCM16）。"""
    http_base = (ws_base or "wss://api.openai.com/v1/realtime")
    http_base = http_base.replace("wss://", "https://").replace("ws://", "http://")
    http_base = http_base.removesuffix("/realtime")
    req = urllib.request.Request(
        f"{http_base}/audio/speech",
        data=json.dumps({
            "model": "gpt-4o-mini-tts",
            "voice": voice,
            "input": PREVIEW_TEXT,
            "response_format": "pcm",
        }).encode(),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


async def _qwen_preview(voice: str, api_key: str, model: str) -> bytes:
    """Qwen：开一次性 realtime 会话读固定文本（音色与对话完全一致）。"""
    from .provider import QwenRealtimeProvider

    p = QwenRealtimeProvider(api_key, model)
    await p.connect()
    await p.update_session({
        "instructions": "你是试听助手。只朗读用户给的文字，一字不改，不加任何内容。",
        "voice": voice,
        "modalities": ["audio", "text"],
        "turn_detection": None,
    })
    await p.inject_text(PREVIEW_TEXT)
    await p.create_response()
    pcm = b""
    async for evt in p.events():
        if evt["type"] == "assistant.audio_delta":
            pcm += evt["pcm"]
        elif evt["type"] == "response.done":
            break
        elif evt["type"] == "error":
            raise RuntimeError(evt.get("error"))
    await p.close()
    return pcm
