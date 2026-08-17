"""M2 WebSocket 桥：把事件总线广播给浏览器 UI，接收浏览器的文本/指令/模式切换。

音频仍走 Python 本机（MicCapture/SpeakerPlayback），浏览器只是渲染层（共享桌面）。
启动：.venv/bin/python -u -m src.main.bridge   然后打开 ui/index.html
UI 指令（在 › 输入行里）：/model [编号|关键词]  /voice [音色名]
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
from src.core.delegate import Delegate
from src.main.cli import MODELS, VOICES, load_env

log = logging.getLogger("bridge")
TOOLS_DIR = "src/tools"
SKILLS_DIR = "src/skills"
KEY_ENV = {"qwen": "DASHSCOPE_API_KEY", "openai": "OPENAI_API_KEY"}
import re
from datetime import datetime
from pathlib import Path
WS_HOST, WS_PORT = "127.0.0.1", 8765
MODEL_TAGS = ["qwen plus", "qwen flash", "gpt 2.1", "gpt mini"]  # 与 MODELS 顺序一致


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
    if name == "deep_think":
        return "主力模型思考中"
    if name == "request_note":
        return f"撰写笔记 {str(args.get('topic', ''))[:18]}".strip()
    if name == "show_note":
        return f"记下笔记 {str(args.get('title', ''))[:18]}".strip()
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


def read_today_notes() -> list[dict]:
    """读取当天落盘笔记（show_note 写的 - [HH:MM] **标题** 内容 格式），供新客户端回填。"""
    path = Path("notes") / f"{datetime.now():%Y-%m-%d}.md"
    if not path.is_file():
        return []
    notes = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^- \[(?P<ts>\d\d:\d\d)\] \*\*(?P<title>.+?)\*\* (?P<content>.*)$", line)
        if m:
            notes.append({"title": m["title"], "content": m["content"], "ts": 0})
    return notes


class Bridge:
    def __init__(self, bus: EventBus, registry: ToolRegistry, hooks: Hooks, delegate: Delegate):
        self.bus = bus
        self.registry = registry
        self.hooks = hooks
        self.clients: set = set()
        self.delegate = delegate
        self.cur = {
            "provider": os.environ.get("VOICE_PROVIDER", "qwen"),
            "model": os.environ.get("VOICE_MODEL", "") or None,
            "voice": {"qwen": "longanqian", "openai": "marin"},
        }
        if self.cur["model"] is None:
            self.cur["model"] = {"qwen": "qwen-audio-3.0-realtime-plus",
                                 "openai": "gpt-realtime-2.1"}.get(self.cur["provider"], "")
        self.session: VoiceSession | None = None
        self.task: asyncio.Task | None = None
        bus.subscribe(self._on_event)
        bus.subscribe(self._console_log)

    # ── 会话生命周期 ──

    def _make_session(self) -> VoiceSession | None:
        key = os.environ.get(KEY_ENV.get(self.cur["provider"], ""), "")
        if not key:
            log.error("缺少 %s", KEY_ENV.get(self.cur["provider"]))
            return None
        provider = create_provider(self.cur["provider"], key, self.cur["model"],
                                   ws_base=os.environ.get("OPENAI_WS_BASE", ""))
        s = VoiceSession(provider, self.bus, tools=self.registry, hooks=self.hooks,
                         skills=load_skills(SKILLS_DIR),
                         config={"voice": self.cur["voice"][self.cur["provider"]]})
        self.delegate.attach(s)
        return s

    async def start(self):
        self.session = self._make_session()
        if self.session:
            self.task = asyncio.create_task(self.session.run())

    async def restart(self):
        """换模型/音色的落点：旧会话死透再建新的（与 CLI 同一套双保险）。"""
        old_task, old_session = self.task, self.session
        if old_task:
            old_task.cancel()
        if old_session:
            await old_session.close()
        if old_task:
            _, pending = await asyncio.wait({old_task}, timeout=3)
            if pending:
                log.warning("旧会话任务 3s 未退出（已置关闭闸）")
        mic_on = old_session.mic_enabled if old_session else True
        self.session = self._make_session()
        if self.session is None:
            return False
        self.session.mic_enabled = mic_on
        self.task = asyncio.create_task(self.session.run())
        return True

    # ── 指令 ──

    async def command(self, text: str) -> str:
        """/model /voice，返回反馈文案（走 hint 通道显示）。"""
        parts = text[1:].split()
        cmd, arg = parts[0].lower(), (parts[1] if len(parts) > 1 else "")
        if cmd == "model":
            if not arg:
                return (f"当前 {self.cur['model']}（可切 "
                        + " · ".join(f"{i+1} {t}" for i, t in enumerate(MODEL_TAGS)) + "）")
            idx = None
            if arg.isdigit() and 1 <= int(arg) <= len(MODELS):
                idx = int(arg) - 1
            else:
                for i, tag in enumerate(MODEL_TAGS):
                    if arg.lower() in tag:
                        idx = i
                        break
            if idx is None:
                return f"不认识模型 {arg}"
            m = MODELS[idx]
            self.cur["provider"], self.cur["model"] = m["provider"], m["model"]
            ok = await self.restart()
            return f"已切换到 {MODEL_TAGS[idx]}" if ok else "切换失败（缺 key）"
        if cmd == "voice":
            voices = VOICES[self.cur["provider"]]
            if not arg:
                return f"当前音色 {self.cur['voice'][self.cur['provider']]}（可切 " + " ".join(voices) + "）"
            if arg not in voices:
                return f"不认识音色 {arg}"
            self.cur["voice"][self.cur["provider"]] = arg
            ok = await self.restart()
            return f"音色换成 {arg}" if ok else "切换失败"
        return f"未知指令（可用 /model /voice）"

    # ── 事件流转 ──

    def _console_log(self, evt):
        if evt.type == "error":
            log.warning("事件 error: %s", evt.data.get("error"))
        elif evt.type in ("reconnected", "trimmed", "mode"):
            log.warning("事件 %s: %s", evt.type, evt.data)

    def _on_event(self, evt):
        """总线事件 → JSON 广播（同步回调里异步发送）。"""
        t = evt.type
        if t == "tool.call":
            msg = {"type": "tool.hint", "text": tool_hint(evt.data.get("name", ""),
                                                          evt.data.get("arguments", ""))}
        elif t == "tool.result":
            msg = {"type": "tool.done"}
        elif t == "worker.tool":  # 主力模型的工具动作也上提示行
            msg = {"type": "tool.hint", "text": "主力 " + tool_hint(evt.data.get("name", ""),
                                                                  evt.data.get("arguments", ""))}
        elif t == "tool.hint":  # delegate 直接发的提示
            msg = {"type": "tool.hint", "text": evt.data.get("text", "")}
        elif t == "user.typed":  # 键盘消息回显进聊天流
            msg = {"type": "msg", "role": "user", "text": evt.data.get("text", "")}
        elif t == "user.transcript_done":  # 语音转写进聊天流
            msg = {"type": "msg", "role": "user", "text": evt.data.get("transcript", "")}
        elif t == "assistant.transcript_done":
            msg = {"type": "msg", "role": "assistant", "text": evt.data.get("transcript", "")}
        elif t == "ui.worker":  # 主力模型的完整 Markdown 输出
            msg = {"type": "msg", "role": "worker", "text": evt.data.get("text", "")}
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

    async def _broadcast_hint(self, text: str):
        payload = json.dumps({"type": "tool.hint", "text": text}, ensure_ascii=False)
        for ws in list(self.clients):
            await self._send(ws, payload)

    async def handler(self, ws):
        self.clients.add(ws)
        if self.session:
            await self._send(ws, json.dumps(
                {"type": "mode", "mic": self.session.mic_enabled}, ensure_ascii=False))
            await self._send(ws, json.dumps(
                {"type": "state", "state": self.session.state.value}, ensure_ascii=False))
        # 历史笔记回填：刷新页面不丢当天的卡片
        for note in read_today_notes():
            await self._send(ws, json.dumps({"type": "ui.note", **note}, ensure_ascii=False))
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("type") == "text" and msg.get("text", "").strip():
                    text = msg["text"].strip()
                    if text.startswith("/"):
                        await self._broadcast_hint(await self.command(text))
                        continue
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

    load_all()
    bridge = Bridge(bus, registry, hooks, Delegate(bus))
    await bridge.start()
    if bridge.session is None:
        return

    async def on_reload():
        names, skill_names, skill_list = load_all()
        bridge.session.skills = skill_list
        await bridge.session.refresh()
        bus.publish("reloaded", tools=names, skills=skill_names)

    reloader = ReloadManager([TOOLS_DIR, SKILLS_DIR], on_change=on_reload)
    reload_task = asyncio.create_task(reloader.run())

    server = await websockets.serve(bridge.handler, WS_HOST, WS_PORT)
    print(f"✓ WebSocket 桥已启动: ws://{WS_HOST}:{WS_PORT}")
    print(f"✓ provider={bridge.cur['provider']} model={bridge.cur['model']}")

    async def report_devices():
        """启动诊断：麦克风是否就绪 + 当前输出设备（耳机路由排查用）。"""
        await asyncio.sleep(6)
        import sounddevice as sd
        mic_ok = bridge.session and bridge.session.mic is not None
        try:
            out = sd.query_devices(sd.default.device[1])["name"]
            inp = sd.query_devices(sd.default.device[0])["name"]
        except Exception:
            out = inp = "?"
        print(f"🎙 麦克风: {'就绪' if mic_ok else '⚠️ 不可用'} | 输入: {inp} | 输出: {out}")

    diag_task = asyncio.create_task(report_devices())
    print("→ 打开 ui/index.html 开始对话；Ctrl+C 退出")

    try:
        await server.wait_closed()
    finally:
        reloader.stop()
        reload_task.cancel()
        diag_task.cancel()
        if bridge.task:
            bridge.task.cancel()
        if bridge.session:
            await bridge.session.close()


def main():
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("\n再见 👋")


if __name__ == "__main__":
    main()
