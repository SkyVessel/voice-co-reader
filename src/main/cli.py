"""CLI 瘦客户端：订阅事件总线，打印状态/转写/延迟。

UI 的全部职责 = 订阅事件 + 渲染。将来 React UI 订阅同一个总线（M2 起经 WebSocket）。
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from src.core.events import Event, EventBus
from src.core.provider import QwenRealtimeProvider
from src.core.session import VoiceSession

STATE_ICON = {"idle": "⏸ ", "listening": "🎙 ", "thinking": "🤔", "speaking": "🔊"}


def load_env(path: str = ".env"):
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def render(evt: Event):
    """把事件渲染到终端。"""
    t, d = evt.type, evt.data
    if t == "state":
        print(f"\n{STATE_ICON.get(d['state'], '?')} [{d['state']}]", flush=True)
    elif t == "user.transcript_delta":
        print(f"\r你: {d['delta']}\033[90m{d.get('stash', '')}\033[0m   ", end="", flush=True)
    elif t == "assistant.transcript_delta":
        print(d["delta"], end="", flush=True)
    elif t == "response.created":
        print("AI: ", end="", flush=True)
    elif t == "interrupted":
        print("\n⚡ [已打断]", flush=True)
    elif t == "latency.ttfa":
        print(f"\n⏱  首音延迟 TTFA: {d['seconds']}s", flush=True)
    elif t == "response.done":
        if d.get("status") != "completed":
            print(f"\n[响应结束: {d.get('status')} {d.get('reason') or ''}]", flush=True)
    elif t == "tool.call":
        print(f"\n🔧 [调用工具: {d.get('name')}]", flush=True)
    elif t == "error":
        print(f"\n❌ {d.get('error')}", flush=True)
    elif t == "session.created":
        s = d.get("session", {})
        print(f"已连接: {s.get('model')} (voice={s.get('voice')})", flush=True)


async def amain():
    load_env()
    api_key = os.environ.get("DASHSCOPE_API_KEY")
    if not api_key:
        sys.exit("缺少 DASHSCOPE_API_KEY（写入 .env 或环境变量）")

    model = os.environ.get("VOICE_MODEL", "qwen-audio-3.0-realtime-plus")
    bus = EventBus()
    bus.subscribe(render)

    provider = QwenRealtimeProvider(api_key=api_key, model=model)
    session = VoiceSession(provider, bus)
    print(f"启动中… 模型={model}，直接开口说话，Ctrl+C 退出")

    try:
        await session.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await session.close()


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("\n再见 👋")


if __name__ == "__main__":
    main()
