"""M2 WebSocket 桥：把事件总线广播给浏览器 UI，接收浏览器的文本/模式切换。

音频仍走 Python 本机（MicCapture/SpeakerPlayback），浏览器只是渲染层（共享桌面）。
启动：.venv/bin/python -u -m src.main.bridge   然后打开 ui/index.html
（/model /voice 等管理操作仍在 CLI 里做，桥只承载对话与展示）
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import websockets

from src.core.events import EventBus
from src.core.hooks import Hooks
from src.core.provider import create_provider
from src.core.reload import ReloadManager
from src.core.session import VoiceSession
from src.core.skills import load_skills
from src.core.tools import ToolContext, ToolRegistry, load_tools_from_dir
from src.main.cli import load_env

log = logging.getLogger("bridge")
TOOLS_DIR = "src/tools"
SKILLS_DIR = "src/skills"
KEY_ENV = {"qwen": "DASHSCOPE_API_KEY", "openai": "OPENAI_API_KEY"}
WS_HOST, WS_PORT = "127.0.0.1", 8765


def tool_hint(name: str, arguments: str) -> str:
    """工具调用 → 人话提示。铁律：零符号——不出现 JSON/路径/引号/冒号/括号。"""
    try:
        args = json.loads(arguments or "{}")
    except Exception:
        args = {}

    def base(p) -> str:
        return os.path.basename(str(p or "")).strip()

    if name == "web_search":
        return f"搜索 {str(args.get('query', ''))[:24]}".strip()
    if name == "web_fetch":
        return "阅读网页"
    if name == "bash":
        return "运行命令"
    if name == "read_file":
        return f"读文件 {base(args.get('path'))}".strip()
    if name == "write_file":
        return f"写文件 {base(args.get('path'))}".strip()
    if name == "edit_file":
        return f"改文件 {base(args.get('path'))}".strip()
    if name == "show_note":
        return f"记下笔记 {str(args.get('title', ''))[:16]}".strip()
    return f"使用工具 {name}"


class Bridge:
    def __init__(self, bus: EventBus, session: VoiceSession):
        self.bus = bus
        self.session = session
        self.clients: set = set()
        bus.subscribe(self._on_event)

    def _on_event(self, evt):
        """总线事件 → JSON 广播（同步回调里异步发送）。"""
        t = evt.type
        if t == "tool.call":
            msg = {"type": "tool.hint", "text": tool_hint(evt.data.get("name", ""),
                                                          evt.data.get("arguments", ""))}
        elif t == "tool.result":
            msg = {"type": "tool.done"}
        elif t in ("state", "levels", "mode", "error", "reconnected",
                   "trimmed", "usage", "ui.note", "interrupted"):
            msg = {"type": t, **evt.data}
        else:
            return  # 转写 delta 等不进 UI（刻意不显示文字流）
        payload = json.dumps(msg, ensure_ascii=False, default=str)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        for ws in list(self.clients):
            loop.create_task(self._send(ws, payload))

    async def _send(self, ws, payload: str):
        try:
            await ws.send(payload)
        except Exception:
            self.clients.discard(ws)

    async def handler(self, ws):
        self.clients.add(ws)
        # 新客户端：同步当前模式与状态
        await self._send(ws, json.dumps(
            {"type": "mode", "mic": self.session.mic_enabled}, ensure_ascii=False))
        await self._send(ws, json.dumps(
            {"type": "state", "state": self.session.state.value}, ensure_ascii=False))
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("type") == "text" and msg.get("text", "").strip():
                    text = msg["text"].strip()
                    self.bus.publish("user.typed", text=text)
                    await self.session.provider.inject_text(text)
                    await self.session.provider.create_response()
                elif msg.get("type") == "mode.toggle":
                    self.session.mic_enabled = not self.session.mic_enabled
                    self.bus.publish("mode", mic=self.session.mic_enabled)
        finally:
            self.clients.discard(ws)


async def amain():
    load_env()
    bus = EventBus()
    hooks = Hooks()
    registry = ToolRegistry(ToolContext(bus))

    def load_all():
        registry.clear()
        load_tools_from_dir(registry, TOOLS_DIR)
        skill_list = load_skills(SKILLS_DIR)
        return registry.names(), [s.name for s in skill_list], skill_list

    _, _, initial_skills = load_all()

    provider_name = os.environ.get("VOICE_PROVIDER", "qwen")
    key = os.environ.get(KEY_ENV.get(provider_name, ""), "")
    if not key:
        print(f"❌ 缺少 {KEY_ENV.get(provider_name)}")
        return
    model = os.environ.get("VOICE_MODEL", "") or None
    provider = create_provider(provider_name, key, model,
                               ws_base=os.environ.get("OPENAI_WS_BASE", ""))
    session = VoiceSession(provider, bus, tools=registry, hooks=hooks, skills=initial_skills)
    bridge = Bridge(bus, session)

    async def on_reload():
        names, skill_names, skill_list = load_all()
        session.skills = skill_list
        await session.refresh()
        bus.publish("reloaded", tools=names, skills=skill_names)

    reloader = ReloadManager([TOOLS_DIR, SKILLS_DIR], on_change=on_reload)
    reload_task = asyncio.create_task(reloader.run())

    server = await websockets.serve(bridge.handler, WS_HOST, WS_PORT)
    print(f"✓ WebSocket 桥已启动: ws://{WS_HOST}:{WS_PORT}")
    print(f"✓ provider={provider_name} model={provider.model}")
    print("→ 打开 ui/index.html 开始对话；Ctrl+C 退出")

    try:
        await asyncio.gather(session.run(), server.wait_closed())
    finally:
        reloader.stop()
        reload_task.cancel()
        await session.close()


def main():
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("\n再见 👋")


if __name__ == "__main__":
    main()
