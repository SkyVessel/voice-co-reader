"""CLI 瘦客户端：订阅事件总线，打印状态/转写/延迟/工具/笔记。

UI 的全部职责 = 订阅事件 + 渲染。将来 React UI 订阅同一个总线（M2 起经 WebSocket）。
harness 组装也在这里：工具目录加载 + 技能加载 + hooks + reload。
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from src.core.events import Event, EventBus
from src.core.hooks import Hooks
from src.core.provider import QwenRealtimeProvider
from src.core.reload import ReloadManager
from src.core.session import VoiceSession
from src.core.skills import load_skills
from src.core.tools import ToolContext, ToolRegistry, load_tools_from_dir

STATE_ICON = {"idle": "⏸ ", "listening": "🎙 ", "thinking": "🤔", "speaking": "🔊"}
TOOLS_DIR = "src/tools"
SKILLS_DIR = "src/skills"


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
    elif t == "ui.note":
        print(f"\n📝 [笔记] {d['title']}: {d['content']}", flush=True)
    elif t == "reloaded":
        print(f"\n♻️  [热重载] 工具: {d.get('tools') or '无'} | 技能: {d.get('skills') or '无'}", flush=True)
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
    hooks = Hooks()
    registry = ToolRegistry(ToolContext(bus))
    session.skills = skills

    def load_all():
        registry.clear()
        load_tools_from_dir(registry, TOOLS_DIR)
        skill_list = load_skills(SKILLS_DIR)
        return registry.names(), [s.name for s in skill_list], skill_list

    load_all()

    async def on_reload():
        names, skill_names, skill_list = load_all()
        session.skills = skill_list
        await session.refresh()
        bus.publish("reloaded", tools=names, skills=skill_names)
        await hooks.emit("on_reload", tools=names, skills=skill_names)

    reloader = ReloadManager([TOOLS_DIR, SKILLS_DIR], on_change=on_reload)

    print(f"启动中… 模型={model}，直接开口说话，Ctrl+C 退出")
    print(f"工具: {registry.names() or '无'} | 技能: {[s.name for s in session.skills] or '无'}")
    print(f"（改 {TOOLS_DIR}/*.py 或 {SKILLS_DIR}/*/SKILL.md 会热重载）")

    reload_task = asyncio.create_task(reloader.run())
    try:
        await session.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        reloader.stop()
        reload_task.cancel()
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
