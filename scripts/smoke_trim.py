"""冒烟：主动裁剪（active trimming）。低阈值（max_history_items=4）连续注入 7 轮文本对话。

验收（对应 docs/计划-下一步.md 任务②）：
  ① conversation.item.delete 被服务端接受（无 error 事件）
  ② 裁剪后模型仍能正常回答
  ③ notes/session-*.md 落盘有完整转写
  ④ usage.input_tokens 不再线性膨胀

用法：VOICE_PROVIDER=qwen VOICE_MODEL=qwen-audio-3.0-realtime-flash .venv/bin/python scripts/smoke_trim.py
"""

from __future__ import annotations

import asyncio
import os
import pathlib
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
    key = os.environ[{"qwen": "DASHSCOPE_API_KEY", "openai": "OPENAI_API_KEY"}[provider_name]]

    bus = EventBus()
    bus.subscribe(render)
    registry = ToolRegistry(ToolContext(bus))
    load_tools_from_dir(registry, "src/tools")
    provider = create_provider(provider_name, key, os.environ.get("VOICE_MODEL", ""),
                               ws_base=os.environ.get("OPENAI_WS_BASE", ""))
    session = VoiceSession(provider, bus, tools=registry, use_mic=False,
                           config={"turn_detection": None, "max_history_items": 4})
    run_task = asyncio.create_task(session.run())
    for _ in range(50):
        if any(e.type == "session.updated" for e in bus.history):
            break
        await asyncio.sleep(0.1)

    done_count = 0

    async def turn(text: str, timeout: int = 30):
        nonlocal done_count
        await provider.inject_text(text)
        await provider.create_response()
        for _ in range(timeout * 10):
            await asyncio.sleep(0.1)
            c = sum(1 for e in bus.history if e.type == "response.done")
            if c > done_count:
                done_count = c
                return
        raise TimeoutError(f"超时: {text}")

    for i in range(1, 8):
        print(f"\n>>> 轮 {i}", flush=True)
        await turn(f"记住数字 {i}。只回答四个字：记住了 {i}" if i < 7
                   else "我最后让你记住的数字是几？只回答数字")

    trims = [e for e in bus.history if e.type == "trimmed"]
    errors = [e.data.get("error") for e in bus.history if e.type == "error"]
    usages = [e.data["usage"] for e in bus.history
              if e.type == "usage" and isinstance(e.data.get("usage"), dict)]
    files = sorted(pathlib.Path("notes").glob("session-*.md"))

    print("\n=== 验收 ===")
    print(f"① 裁剪事件: {len(trims)} 次", "✓" if trims else "✗ 未触发")
    if trims:
        print(f"   最后一次: {trims[-1].data}")
    print(f"   服务端错误: {errors or '无'}", "✓" if not errors else "✗")
    print(f"② 裁剪后第 7 轮仍正常应答 ✓（若上方有回答）")
    print(f"③ 归档文件: {[str(f) for f in files] or '无'}", "✓" if files else "✗")
    if files:
        content = files[-1].read_text(encoding="utf-8")
        print(f"   内容 {len(content)} 字符, 前 300:\n{content[:300]}")
    if usages:
        traj = [u.get("input_tokens") for u in usages]
        print(f"④ input_tokens 轨迹: {traj}")
        print("   ", "✓ 趋于平稳" if len(traj) > 3 and traj[-1] < traj[2] * 1.5 else "   ⚠️ 仍在膨胀")
    print(f"   裁剪后本地流水剩余: {len(session._conv)} 项")

    run_task.cancel()
    await session.close()


asyncio.run(main())
