"""冒烟：调研体验闭环（任务③验收）。

注入一个调研请求，期望观察到完整链路：
  web_search → web_fetch → 口述摘要 → show_note（→ notes/YYYY-MM-DD.md 落盘，含来源 URL）

用法：VOICE_PROVIDER=qwen VOICE_MODEL=qwen-audio-3.0-realtime-flash .venv/bin/python scripts/smoke_research.py
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
from src.core.skills import load_skills
from src.core.tools import ToolContext, ToolRegistry, load_tools_from_dir
from src.main.cli import load_env, render

QUESTION = "帮我调研一下 2026 年语音 AI agent 的市场格局，主要玩家和定价"


async def main():
    load_env()
    provider_name = os.environ.get("VOICE_PROVIDER", "qwen")
    key = os.environ[{"qwen": "DASHSCOPE_API_KEY", "openai": "OPENAI_API_KEY"}[provider_name]]

    bus = EventBus()
    bus.subscribe(render)
    registry = ToolRegistry(ToolContext(bus))
    load_tools_from_dir(registry, "src/tools")
    skills = load_skills("src/skills")
    print(f"技能: {[s.name for s in skills]}")

    provider = create_provider(provider_name, key, os.environ.get("VOICE_MODEL", ""),
                               ws_base=os.environ.get("OPENAI_WS_BASE", ""))
    session = VoiceSession(provider, bus, tools=registry, skills=skills, use_mic=False,
                           config={"turn_detection": None})
    run_task = asyncio.create_task(session.run())
    for _ in range(50):
        if any(e.type == "session.updated" for e in bus.history):
            break
        await asyncio.sleep(0.1)

    print(f"\n>>> 注入调研请求: {QUESTION}")
    await provider.inject_text(QUESTION)
    await provider.create_response()

    # 调研是多轮工具链：等到连续 20s 没有新的工具调用/响应活动视为收尾
    last_activity = asyncio.get_event_loop().time()
    seen = 0
    for _ in range(1800):  # 上限 3 分钟
        await asyncio.sleep(0.1)
        hist = bus.history
        if len(hist) > seen:
            seen = len(hist)
            if any(e.type in ("tool.call", "response.done", "ui.note") for e in hist[seen - 5:]):
                last_activity = asyncio.get_event_loop().time()
        if asyncio.get_event_loop().time() - last_activity > 20:
            break

    tools = [e.data.get("name") for e in bus.history if e.type == "tool.call"]
    notes = [e.data for e in bus.history if e.type == "ui.note"]
    files = sorted(pathlib.Path("notes").glob("????-??-??.md"))

    print("\n=== 验收 ===")
    print(f"工具链: {tools}")
    print(f"① web_search 被调用: {'✓' if 'web_search' in tools else '✗'}")
    print(f"② web_fetch 被调用: {'✓' if 'web_fetch' in tools else '✗（可能搜索摘要已够用）'}")
    print(f"③ show_note 笔记: {len(notes)} 条 {'✓' if notes else '✗'}")
    for n in notes:
        has_url = "http" in (n.get("content") or "")
        print(f"   - {n.get('title')}: {(n.get('content') or '')[:80]} {'🔗' if has_url else '⚠️无URL'}")
    today = [f for f in files if f.stem == __import__("datetime").date.today().isoformat()]
    if today:
        content = today[-1].read_text(encoding="utf-8")
        print(f"④ 落盘 {today[-1]}: {len(content)} 字符 {'✓' if content.strip() else '✗ 空'}")
    else:
        print("④ 落盘: ✗ 当日笔记文件不存在")

    run_task.cancel()
    await session.close()


asyncio.run(main())
