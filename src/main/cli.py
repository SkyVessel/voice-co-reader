"""CLI 瘦客户端：订阅事件总线，打印状态/转写/延迟/工具/笔记。

UI 的全部职责 = 订阅事件 + 渲染。将来 React UI 订阅同一个总线（M2 起经 WebSocket）。
harness 组装也在这里：工具目录加载 + 技能加载 + hooks + reload。

操作：
- 开口说话（语音模式）或直接打字回车（任何模式）
- Shift+Tab 切换 语音+键盘 / 仅键盘 模式
- /model 切换模型（重连会话）；/voice 滑块选音色（←→ 试听，Enter 确认重连）
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from src.core.events import Event, EventBus
from src.core.hooks import Hooks
from src.core.provider import create_provider
from src.core.reload import ReloadManager
from src.core.session import VoiceSession
from src.core.skills import load_skills
from src.core.tools import ToolContext, ToolRegistry, load_tools_from_dir
from src.core.tts import voice_preview

STATE_ICON = {"idle": "⏸ ", "listening": "🎙 ", "thinking": "🤔", "speaking": "🔊"}
TOOLS_DIR = "src/tools"
SKILLS_DIR = "src/skills"

# 模型菜单（全是 realtime 语音模型；文本模型需要管线模式，不在此列）
MODELS = [
    {"provider": "qwen", "model": "qwen-audio-3.0-realtime-plus",
     "desc": "质量优先 · agentic 最强（默认）"},
    {"provider": "qwen", "model": "qwen-audio-3.0-realtime-flash",
     "desc": "便宜 · 更快"},
    {"provider": "openai", "model": "gpt-realtime-2.1",
     "desc": "OpenAI 最新（经中转，通道有漂移）"},
    {"provider": "openai", "model": "gpt-realtime-2.1-mini",
     "desc": "OpenAI 最便宜（经中转）"},
]
# 音色列表（官方文档核读：qwen realtime 5 系统音色；openai 经典 8 + marin/cedar）
VOICES = {
    "qwen": ["longanqian", "longanlingxin", "longanlingxi", "longanxiaoxin", "longanlufeng"],
    "openai": ["marin", "alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse", "cedar"],
}
KEY_ENV = {"qwen": "DASHSCOPE_API_KEY", "openai": "OPENAI_API_KEY"}


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
    elif t == "mode":
        print(f"\n🔀 [模式] {'🎙 语音+键盘' if d['mic'] else '⌨️ 仅键盘（麦克风已静音）'}", flush=True)
    elif t == "user.typed":
        print(f"你(键盘): {d['text']}", flush=True)
    elif t == "latency.ttfa":
        print(f"\n⏱  首音延迟 TTFA: {d['seconds']}s", flush=True)
    elif t == "response.done":
        if d.get("status") != "completed":
            print(f"\n[响应结束: {d.get('status')} {d.get('reason') or ''}]", flush=True)
    elif t == "reconnected":
        print("\n🔁 [已自动重连]（对话上下文已重置，可继续聊）", flush=True)
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
        if isinstance(s, dict):
            voice = (s.get("audio") or {}).get("output", {}).get("voice") or s.get("voice", "?")
            print(f"已连接: {s.get('model', '?')} (voice={voice})", flush=True)


async def amain():
    load_env()
    bus = EventBus()
    bus.subscribe(render)
    hooks = Hooks()
    registry = ToolRegistry(ToolContext(bus))

    def load_all():
        registry.clear()
        load_tools_from_dir(registry, TOOLS_DIR)
        skill_list = load_skills(SKILLS_DIR)
        return registry.names(), [s.name for s in skill_list], skill_list

    _, _, initial_skills = load_all()

    # ── 会话状态（可经 /model /voice 重建）──
    cur = {
        "provider": os.environ.get("VOICE_PROVIDER", "qwen"),
        "model": os.environ.get("VOICE_MODEL", "") or None,
        "voice": {"qwen": "longanqian", "openai": "marin"},
        "session": None,
        "task": None,
    }
    if cur["model"] is None:
        cur["model"] = {"qwen": "qwen-audio-3.0-realtime-plus",
                        "openai": "gpt-realtime-2.1"}.get(cur["provider"], "")

    def make_session():
        key = os.environ.get(KEY_ENV.get(cur["provider"], ""), "")
        if not key:
            print(f"❌ 缺少 {KEY_ENV.get(cur['provider'])}")
            return None
        provider = create_provider(cur["provider"], key, cur["model"],
                                   ws_base=os.environ.get("OPENAI_WS_BASE", ""))
        return VoiceSession(provider, bus, tools=registry, hooks=hooks,
                            skills=initial_skills,
                            config={"voice": cur["voice"][cur["provider"]]})

    async def restart_session():
        """关闭旧会话并按当前 cur 配置重连（/model /voice 的落点）。"""
        old_task, old_session = cur["task"], cur["session"]
        if old_task:
            old_task.cancel()
        if old_session:
            await old_session.close()  # 置 _closed 闸 + 关麦/扬声器/ws
        if old_task:
            # 确定性等待旧任务死亡（asyncio.wait 不传播任务的 CancelledError）
            done, pending = await asyncio.wait({old_task}, timeout=3)
            if pending:
                print("⚠️ 旧会话任务未在 3s 内退出（已置关闭闸，不会重连）")
        session = make_session()
        if session is None:
            return
        if cur["session"] is not None:
            session.mic_enabled = cur["session"].mic_enabled  # 保留键盘/语音模式
        session.skills = load_skills(SKILLS_DIR)
        cur["session"] = session
        cur["task"] = asyncio.create_task(session.run())

    await restart_session()
    if cur["session"] is None:
        sys.exit(1)

    async def on_reload():
        names, skill_names, skill_list = load_all()
        cur["session"].skills = skill_list
        await cur["session"].refresh()
        bus.publish("reloaded", tools=names, skills=skill_names)
        await hooks.emit("on_reload", tools=names, skills=skill_names)

    reloader = ReloadManager([TOOLS_DIR, SKILLS_DIR], on_change=on_reload)

    # ── 键盘输入（prompt_toolkit）──
    from prompt_toolkit import PromptSession
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.patch_stdout import patch_stdout

    kb = KeyBindings()

    @kb.add("s-tab")  # Shift+Tab
    def _toggle(event):
        cur["session"].mic_enabled = not cur["session"].mic_enabled
        bus.publish("mode", mic=cur["session"].mic_enabled)

    ps = PromptSession(key_bindings=kb)

    def toolbar():
        return "🎙 语音+键盘 | Shift+Tab 切模式 | /model /voice" if cur["session"].mic_enabled \
            else "⌨️  仅键盘（他听不到你，你能听到他）| Shift+Tab 切模式 | /model /voice"

    async def cmd_model():
        print("\n可用模型：")
        for i, m in enumerate(MODELS, 1):
            mark = " ← 当前" if (m["provider"] == cur["provider"] and m["model"] == cur["model"]) else ""
            print(f"  {i}. {m['model']}  [{m['provider']}] {m['desc']}{mark}")
        try:
            ans = await ps.prompt_async("选编号（回车取消）> ")
        except (EOFError, KeyboardInterrupt):
            return
        ans = ans.strip()
        if not ans.isdigit() or not (1 <= int(ans) <= len(MODELS)):
            return
        m = MODELS[int(ans) - 1]
        cur["provider"], cur["model"] = m["provider"], m["model"]
        print(f"切换中… {m['model']}（会断开当前会话）")
        await restart_session()

    async def cmd_voice():
        voices = VOICES[cur["provider"]]
        current = cur["voice"][cur["provider"]]
        sel = {"i": voices.index(current) if current in voices else 0, "cancel": False}
        vk = KeyBindings()

        def move(delta):
            sel["i"] = (sel["i"] + delta) % len(voices)
            asyncio.get_running_loop().create_task(preview(voices[sel["i"]]))

        @vk.add("left")
        def _left(event):
            move(-1)

        @vk.add("right")
        def _right(event):
            move(1)

        @vk.add("escape")
        def _esc(event):
            sel["cancel"] = True
            event.app.exit()

        def slider():
            cells = [f"【{v}】" if j == sel["i"] else v for j, v in enumerate(voices)]
            return "◀ " + " ".join(cells) + " ▶   ←→ 切换并试听 · Enter 确认 · Esc 取消"

        vps = PromptSession(key_bindings=vk)
        print("\n选音色（移动即试听）：")
        try:
            await vps.prompt_async("> ", bottom_toolbar=slider)
        except (EOFError, KeyboardInterrupt):
            sel["cancel"] = True
        if sel["cancel"]:
            print("已取消")
            return
        chosen = voices[sel["i"]]
        if chosen != cur["voice"][cur["provider"]]:
            cur["voice"][cur["provider"]] = chosen
            print(f"音色 → {chosen}，重连生效…")
            await restart_session()
        else:
            print("音色未变")

    async def preview(voice: str):
        key = os.environ.get(KEY_ENV[cur["provider"]], "")
        pcm = await voice_preview(cur["provider"], voice, key,
                                  ws_base=os.environ.get("OPENAI_WS_BASE", ""),
                                  model=cur["model"] if cur["provider"] == "qwen" else "")
        if pcm and cur["session"]:
            cur["session"].speaker.clear()
            cur["session"].speaker.enqueue(pcm)
        elif not pcm:
            print(f"（{voice} 试听生成失败）")

    async def keyboard_loop():
        while True:
            try:
                text = await ps.prompt_async("你> ", bottom_toolbar=toolbar)
            except (EOFError, KeyboardInterrupt):
                break
            text = text.strip()
            if not text:
                continue
            if text.startswith("/"):
                cmd = text.split()[0].lower()
                if cmd == "/model":
                    await cmd_model()
                elif cmd == "/voice":
                    await cmd_voice()
                else:
                    print(f"未知命令 {cmd}（可用: /model /voice）")
                continue
            bus.publish("user.typed", text=text)
            await cur["session"].provider.inject_text(text)
            await cur["session"].provider.create_response()

    print(f"启动中… provider={cur['provider']} model={cur['model']}")
    print(f"工具: {registry.names() or '无'} | 技能: {[s.name for s in cur['session'].skills] or '无'}")
    print("操作: 说话 或 打字回车；Shift+Tab 切模式；/model 换模型；/voice 换音色；Ctrl+C 退出")

    reload_task = asyncio.create_task(reloader.run())
    try:
        with patch_stdout():
            await keyboard_loop()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        reloader.stop()
        reload_task.cancel()
        if cur["task"]:
            cur["task"].cancel()
        if cur["session"]:
            await cur["session"].close()


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
