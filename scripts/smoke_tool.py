"""冒烟测试：Function Calling 全链路（文本注入 → 工具调用 → 结果写回 → 口述）。

用法：.venv/bin/python scripts/smoke_tool.py ["问题"]
默认问题会触发 get_current_time 工具。
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.events import EventBus
from src.core.provider import create_provider
from src.core.session import VoiceSession
from src.core.tools import ToolContext, ToolRegistry, load_tools_from_dir
from src.main.cli import load_env, render


async def main():
    load_env()
    provider_name = os.environ.get("VOICE_PROVIDER", "qwen")
    key_env = {"qwen": "DASHSCOPE_API_KEY", "openai": "OPENAI_API_KEY"}.get(provider_name)
    key = os.environ[key_env]
    question = sys.argv[1] if len(sys.argv) > 1 else "现在几点了？今天星期几？"

    bus = EventBus()
    bus.subscribe(render)
    registry = ToolRegistry(ToolContext(bus))
    load_tools_from_dir(registry, "src/tools")
    print(f"已加载工具: {registry.names()}")

    provider = create_provider(provider_name, key, os.environ.get("VOICE_MODEL", ""),
                               ws_base=os.environ.get("OPENAI_WS_BASE", ""))
    print(f"provider={provider_name} model={provider.model}")
    # push-to-talk 模式：手动注入文本 + 触发推理，不走 VAD
    session = VoiceSession(provider, bus, tools=registry, use_mic=False,
                           config={"turn_detection": None})

    run_task = asyncio.create_task(session.run())
    # 等连接 + session.update 完成
    for _ in range(50):
        if any(e.type == "session.updated" for e in bus.history):
            break
        await asyncio.sleep(0.1)

    print(f"\n>>> 注入问题: {question}")
    await provider.inject_text(question)
    await provider.create_response()

    # 等两轮响应（工具调用轮 + 口述轮）或超时
    done_count = 0
    for _ in range(400):
        await asyncio.sleep(0.1)
        done_count = sum(
            1 for e in bus.history
            if e.type == "response.done" and e.data.get("status") in ("completed", "failed")
        )
        tool_called = any(e.type == "tool.call" for e in bus.history)
        if done_count >= (2 if tool_called else 1):
            break

    tool_events = [e for e in bus.history if e.type == "tool.call"]
    print(f"\n=== 结果 ===")
    print(f"工具调用: {[e.data.get('name') for e in tool_events] or '未触发'}")
    print(f"响应轮数: {done_count}")

    run_task.cancel()
    await session.close()


asyncio.run(main())
