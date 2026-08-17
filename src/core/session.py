"""会话层：状态机 + 编排（音频 ↔ Provider ↔ 事件总线 ↔ 工具 ↔ 钩子）。

状态机：IDLE → LISTENING → THINKING → SPEAKING → IDLE
打断（barge-in）：SPEAKING 时收到 user.speech_started → 清空播放缓冲 → LISTENING
延迟口径：TTFA = 首个 assistant.audio_delta 到达时刻 − user.speech_stopped 到达时刻
热更新：refresh() 重发 session.update（tools/instructions 会话中可改）
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from enum import Enum
from pathlib import Path

from .audio import MicCapture, SpeakerPlayback
from .events import EventBus
from .hooks import Hooks
from .provider import RealtimeProvider
from .skills import Skill, format_for_instructions
from .tools import ToolRegistry

log = logging.getLogger("session")


class State(Enum):
    IDLE = "idle"
    LISTENING = "listening"
    THINKING = "thinking"
    SPEAKING = "speaking"


BASE_INSTRUCTIONS = (
    "你是用户的语音前台和导读伙伴。你的职责（都要快，一秒内开口）："
    "寒暄与确认（“我看看”“稍等”）、简短事实问答、导读讲解和划重点、"
    "回答关于已有卡片的追问、任务进度播报。"
    "后台有秘书和主力模型负责慢活：卡片和笔记的生成、检索抓取和长文阅读、"
    "深度推理和结构化输出、跨多份材料的综合、口播稿撰写。"
    "复杂的、量大的信息（绝大多数情况）交给后台；简单、即时的你自己答。"
    "合作方式：接到复杂任务先口头回应一句（比如“我看看，稍等”），然后调 deep_think 派单；"
    "主力模型的详细结果会直接显示在用户屏幕上，结果上屏后你用一两句话讲解要点，不要逐字念。"
    "口头纪律：不要机械朗读链接、编号列表或大段条目——交给屏幕和笔记，"
    "嘴上用自然连接词概括（比如“主要是两点，一是…二是…”）。"
    "分寸：屏幕上的笔记和结果要详细（数据、结论、来源留住），你嘴上说的要简洁。"
    "记笔记：内容多调 request_note 让秘书代写；一句话的小结论自己调 show_note 直接记。"
    "调用工具前，先用一句自然的话预告（比如“我先查一下”），每次换句式。"
    "有可用工具时优先调用工具获取准确信息，不要凭记忆编造。"
)


class VoiceSession:
    def __init__(self, provider: RealtimeProvider, bus: EventBus,
                 tools: ToolRegistry, hooks: Hooks | None = None,
                 skills: list[Skill] | None = None,
                 config: dict | None = None,
                 use_mic: bool = True):
        self.provider = provider
        self.bus = bus
        self.tools = tools
        self.hooks = hooks or Hooks()
        self.skills = skills or []
        self.extra_config = config or {}
        self.use_mic = use_mic
        self.state = State.IDLE
        self._t_stop: float | None = None       # speech_stopped 到达时刻
        self._first_audio = False               # 本轮是否已见首个音频帧
        self.mic: MicCapture | None = None
        self.speaker = SpeakerPlayback()
        self._session_started = False
        self._pending_tool_results: list[tuple[str, str]] = []  # (call_id, result) 待写回
        self._response_active = False          # response.created/done 之间为 True
        self.mic_enabled = True                # False = 键盘模式（丢弃麦克帧）
        self._closed = False                   # close() 后置真：禁止监督循环重连（防僵尸会话）
        self._conv: list[dict] = []                  # 会话项流水 {item_id, who, text}（主动裁剪）
        self._tool_result_text: dict[str, str] = {}  # call_id → 结果文本（item.created 回声时回填）
        self._archive_notified = False               # 是否已告知模型“有归档文件可查”

    def _instructions(self) -> str:
        return BASE_INSTRUCTIONS + format_for_instructions(self.skills)

    def _set_state(self, s: State):
        if s is not self.state:
            self.state = s
            self.bus.publish("state", state=s.value)

    async def run(self):
        loop = asyncio.get_running_loop()
        self.speaker.start()
        tasks = [self._run_connection(), self._levels()]
        if self.use_mic:
            tasks.append(self._start_mic_safely(loop))
        await asyncio.gather(*tasks)

    async def _run_connection(self):
        """连接监督循环：断线（服务器掐断/网络抖动/中转漂移）后指数退避自动重连。
        重连后重发完整 session 配置；对话上下文随旧会话丢失（realtime 无 resume）。"""
        delay = 2
        first = True
        while True:
            if self._closed:
                return
            try:
                await self.provider.connect()
                self._session_started = False   # 新连接必须重发完整配置（voice/turn_detection）
                await self.refresh()
                self._session_started = True
                delay = 2
                if first:
                    first = False
                else:
                    self.bus.publish("reconnected")
                await self._downlink()  # 正常返回 = 连接已断
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.bus.publish("error", error={"type": "connect_failed", "message": str(e)})
            if self._closed:
                return  # close() 在我们断开期间被调用：不要重连
            # 落点：连接已死。清理跨会话无效的状态
            self._pending_tool_results.clear()
            self._response_active = False
            self._conv.clear()              # 重连 = 服务端上下文已丟，本地流水同步重置
            self._tool_result_text.clear()
            self._archive_notified = False
            self._set_state(State.IDLE)
            self.bus.publish("error", error={"type": "reconnecting",
                                             "message": f"{delay}s 后自动重连…"})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def refresh(self):
        """热刷新会话配置：tools + instructions（reload 的落点）。"""
        config = {
            "modalities": ["audio", "text"],
            "voice": "longanqian",
            "instructions": self._instructions(),
            "tools": self.tools.schemas(),
            "turn_detection": {"type": "server_vad", "threshold": 0.5, "silence_duration_ms": 800},
            **self.extra_config,
        }
        if self._session_started:
            # 会话已开始后，turn_detection/voice 不可改，热刷新只更新可改字段
            config.pop("turn_detection", None)
            config.pop("voice", None)
        await self.provider.update_session(config)

    async def _start_mic_safely(self, loop: asyncio.AbstractEventLoop):
        """麦克风初始化单独隔离：卡住/失败不阻塞接听链路（macOS 首次授权弹窗会阻塞）。"""
        def _init():
            mic = MicCapture(loop)
            mic.start()
            return mic
        try:
            self.mic = await asyncio.wait_for(loop.run_in_executor(None, _init), timeout=5)
        except Exception as e:
            self.bus.publish("error", error={"type": "mic_unavailable",
                                             "message": f"麦克风不可用，仅接收模式: {e}"})
            return
        await self._uplink()

    async def _uplink(self):
        """麦克帧 → provider。单帧发送失败只丢帧不杀任务（断线期丢弃，重连后自动恢复）。
        ⚠️ 本方法曾被锚点编辑误吞导致麦克风全哑——改动后请跑 scripts/smoke_tool.py 回归。"""
        while True:
            pcm = await self.mic.frames.get()
            if not self.mic_enabled:  # 键盘模式：丢弃麦克帧（AI 听不到你）
                continue
            try:
                await self.provider.send_audio(pcm)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("uplink 丢帧: %s", e)

    async def _downlink(self):
        try:
            async for evt in self.provider.events():
                await self._handle(evt)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("downlink ended with error")
            self.bus.publish("error", error={"type": "connection_lost", "message": str(e)})
        else:
            log.warning("downlink returned WITHOUT exception, ws state=%s",
                        self.provider._ws.state.name if self.provider._ws else None)

    async def _handle(self, evt: dict):
        t = evt["type"]

        if t == "user.speech_started":
            if self.state is State.SPEAKING:
                self.speaker.clear()          # 打断：立即静音
                self.bus.publish("interrupted")
                await self.hooks.emit("on_interrupt")
            self._set_state(State.LISTENING)
            await self.hooks.emit("on_turn_start")
            self.bus.publish("user.speech_started")

        elif t == "user.speech_stopped":
            self._t_stop = time.time()
            self._first_audio = False
            self._set_state(State.THINKING)

        elif t == "user.transcript_delta":
            self.bus.publish("user.transcript_delta",
                             delta=evt.get("delta", ""), stash=evt.get("stash", ""))

        elif t == "assistant.audio_delta":
            pcm = evt.get("pcm", b"")
            self.speaker.enqueue(pcm)
            if not self._first_audio:
                self._first_audio = True
                self._set_state(State.SPEAKING)
                if self._t_stop:
                    ttfa = time.time() - self._t_stop
                    self.bus.publish("latency.ttfa", seconds=round(ttfa, 3))
                    self._t_stop = None

        elif t == "assistant.transcript_delta":
            self.bus.publish("assistant.transcript_delta", delta=evt.get("delta", ""))

        elif t == "user.committed":
            self._conv.append({"item_id": evt.get("item_id"), "who": "user", "text": ""})

        elif t == "user.transcript_done":
            iid = evt.get("item_id")
            target = next((x for x in reversed(self._conv)
                           if x["who"] == "user" and (x["item_id"] == iid or not x["text"])), None)
            if target:
                if iid and not target["item_id"]:
                    target["item_id"] = iid
                target["text"] = evt.get("transcript", "")
            self.bus.publish("user.transcript_done", transcript=evt.get("transcript", ""),
                             item_id=iid)

        elif t == "assistant.transcript_done":  # Qwen：助手最终转写（含 item_id）
            self._conv.append({"item_id": evt.get("item_id"), "who": "assistant",
                               "text": evt.get("transcript", "")})
            self.bus.publish("assistant.transcript_done", transcript=evt.get("transcript", ""),
                             item_id=evt.get("item_id"))

        elif t == "item.done":  # OpenAI：非工具输出项（含助手最终转写）
            item = evt.get("item", {})
            text = "".join(c.get("transcript") or c.get("text") or ""
                           for c in (item.get("content") or []))
            self._conv.append({"item_id": item.get("id"),
                               "who": item.get("role", "assistant"), "text": text})

        elif t == "item.created":  # 客户端创建项的回声（注入文本 / 工具结果）
            item = evt.get("item", {})
            it_type = item.get("type")
            if it_type == "function_call_output":
                text = self._tool_result_text.pop(item.get("call_id"), "")
                self._conv.append({"item_id": item.get("id"), "who": "tool",
                                   "text": f"工具结果: {text[:300]}"})
            elif it_type == "message":
                text = "".join(c.get("text") or "" for c in (item.get("content") or []))
                if not any(x["item_id"] == item.get("id") for x in self._conv):
                    self._conv.append({"item_id": item.get("id"),
                                       "who": item.get("role", "user"), "text": text})

        elif t == "tool.call":
            # Function Calling：执行 → 结果暂存 → 等 response.done 后统一写回+触发二轮
            # （实测：OpenAI 经典 API 经中转网关时，响应进行中写 item 会被静默吞掉，
            #  全部推到 done 后写是唯一稳定顺序，对 Qwen 同样合法）
            name, args = evt["name"], evt.get("arguments", "{}")
            await self.hooks.emit("on_tool_call", name=name, arguments=args)
            self.bus.publish("tool.call", name=name, arguments=args)
            result = await self.tools.dispatch(name, args)
            await self.hooks.emit("on_tool_result", name=name, result=result)
            self.bus.publish("tool.result", name=name, result=result)
            self._pending_tool_results.append((evt["call_id"], result))
            self._conv.append({"item_id": evt.get("item_id"), "who": "tool",
                               "text": f"调用 {name}: {args[:200]}"})

        elif t == "response.created":
            self._response_active = True
            self.bus.publish("response.created")

        elif t == "response.done":
            self._response_active = False
            status = evt.get("status")
            self.bus.publish("response.done", status=status, reason=evt.get("reason"))
            if evt.get("usage"):
                self.bus.publish("usage", usage=evt["usage"])
            await self.hooks.emit("on_turn_end", status=status)
            self._set_state(State.IDLE)
            if self._pending_tool_results:
                pending, self._pending_tool_results = self._pending_tool_results, []
                asyncio.create_task(self._flush_tool_results(pending))
            elif status == "completed":
                await self._maybe_trim()

        elif t == "error":
            err = evt.get("error")
            # 裁剪删除的良性报错（级联删除：删用户项时服务端已连带删了响应项）不扰民
            if isinstance(err, dict) and err.get("param") == "conversation.item.delete":
                log.info("裁剪删除被服务端跳过（已不存在）: %s", err.get("message"))
            else:
                self.bus.publish("error", error=err)

        elif t in ("session.created", "session.updated"):
            self.bus.publish(t, session=evt.get("session", {}))

    async def _levels(self):
        """10Hz 输出双路电平，UI 律动用；每秒顺带检测输出设备切换（主线程侧，安全）。"""
        tick = 0
        while True:
            await asyncio.sleep(0.1)
            tick += 1
            if tick >= 10:
                tick = 0
                self.speaker.maybe_follow()
            self.bus.publish("levels",
                             mic=round(self.mic.level, 3) if self.mic else 0.0,
                             speaker=round(self.speaker.level, 3))

    async def _flush_tool_results(self, pending: list[tuple[str, str]]):
        """response.done 后：统一写回工具结果 → 触发二轮推理（带确认重试）。
        背景：中转网关（实测 CometAPI/new-api）会静默丢消息，重试对官方 API 无害。"""
        try:
            for call_id, result in pending:
                print(f"[flush] 写回 {call_id[-6:]} ({len(result)} 字符)", flush=True)
                self._tool_result_text[call_id] = result
                await self.provider.write_tool_result(call_id, result)
            for attempt in range(3):
                await self.provider.create_response()
                for _ in range(30):  # 等 3s 看 response.created 是否回来
                    if self._response_active:
                        return
                    await asyncio.sleep(0.1)
                log.warning("response.create 未确认，重试 %d/3", attempt + 2)
            self.bus.publish("error", error={"type": "response_create_lost"})
        except Exception as e:
            # 连接已死（如服务器内容审查掐线）：不再往死 socket 上写，交给重连循环
            self.bus.publish("error", error={"type": "tool_writeback_failed", "message": str(e)})

    async def _maybe_trim(self):
        """主动裁剪（pi compaction 的语音版）：历史超阈值 → 转写落盘 notes/ + 删模型侧 item。
        音频 token 12.5 tok/s 且重复计费，不裁剪 ≈ 只能聊 10 分钟。"""
        max_items = int(self.extra_config.get("max_history_items", 30))
        if len(self._conv) <= max_items:
            return
        cut = len(self._conv) - max_items
        while cut < len(self._conv) and self._conv[cut]["who"] == "tool":
            cut += 1  # 切口前移到非 tool 项，避免切断 调用/结果 对
        old, self._conv = self._conv[:cut], self._conv[cut:]
        # 系统提示项（归档通知）永久钉住：不参与删除与归档
        pinned = [x for x in old if x["who"] == "system"]
        old = [x for x in old if x["who"] != "system"]
        self._conv = pinned + self._conv
        if not old:
            return
        archive_dir = Path(self.extra_config.get("archive_dir", "notes"))
        archive_dir.mkdir(parents=True, exist_ok=True)
        path = archive_dir / f"session-{datetime.now():%Y%m%d}.md"
        with path.open("a", encoding="utf-8") as f:
            for it in old:
                if it["text"]:
                    f.write(f"- [{datetime.now():%H:%M}] **{it['who']}** {it['text']}\n")
        deleted = 0
        for it in old:
            if it.get("item_id"):
                try:
                    await self.provider.delete_item(it["item_id"])
                    deleted += 1
                except Exception as e:
                    log.warning("delete_item 失败（裁剪中止，下轮重试）: %s", e)
                    break
        if not self._archive_notified:
            self._archive_notified = True
            try:
                await self.provider.inject_text(
                    f"[系统提示] 早期对话已归档到文件 {path}，用户问起时可用 read_file 工具查阅。",
                    role="system")
            except Exception:
                pass
        self.bus.publish("trimmed", count=len(old), deleted=deleted, archive=str(path))
    async def close(self):
        self._closed = True  # 先落闸：即使 cancel 竞态漏送，监督循环也不会重连
        if self.mic:
            self.mic.stop()
        self.speaker.stop()
        await self.provider.close()
