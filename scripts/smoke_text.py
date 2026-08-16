"""冒烟测试：不用麦克风，文本注入 → 验证音频/转写返回 + 测 TTFA。

用法：.venv/bin/python scripts/smoke_text.py ["问题"]
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import websockets

from src.main.cli import load_env


async def main():
    load_env()
    key = os.environ["DASHSCOPE_API_KEY"]
    model = os.environ.get("VOICE_MODEL", "qwen-audio-3.0-realtime-plus")
    question = sys.argv[1] if len(sys.argv) > 1 else "用一句话介绍你自己"

    url = f"wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime?model={model}"
    async with websockets.connect(url, additional_headers={"Authorization": f"Bearer {key}"},
                                  open_timeout=15) as ws:
        t0 = time.time()
        await ws.recv()  # session.created
        print(f"✓ session.created ({time.time()-t0:.2f}s)")

        await ws.send(json.dumps({"type": "session.update", "session": {
            "modalities": ["audio", "text"], "voice": "longanqian",
            "turn_detection": None,  # push-to-talk：手动控制
        }}))
        await ws.send(json.dumps({"type": "conversation.item.create", "item": {
            "type": "message", "role": "user",
            "content": [{"type": "input_text", "text": question}]}}))
        t_ask = time.time()
        await ws.send(json.dumps({"type": "response.create"}))

        audio_bytes = 0
        transcript = ""
        first_audio_at = None
        deadline = time.time() + 30
        while time.time() < deadline:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30 - (time.time() - t_ask)))
            t = msg.get("type")
            if t == "response.audio.delta":
                if first_audio_at is None:
                    first_audio_at = time.time()
                audio_bytes += len(base64.b64decode(msg["delta"]))
            elif t == "response.audio_transcript.delta":
                transcript += msg.get("delta", "")
            elif t == "response.done":
                status = msg["response"].get("status")
                break
            elif t == "error":
                print("❌", msg["error"])
                return

        ttfa = first_audio_at - t_ask if first_audio_at else float("nan")
        print(f"✓ response.done status={status}")
        print(f"✓ TTFA(文本→首音频帧): {ttfa:.2f}s | 音频 {audio_bytes/2/24000:.1f}s")
        print(f"✓ 转写: {transcript[:120]}")


asyncio.run(main())
