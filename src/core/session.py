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
from enum import Enum

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
    "你是一个语音知识助手。回答简洁口语化，像面对面交谈一样自然，"
    "避免 markdown 和长列表；不确定的事情直接说不确定。"
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
            # 落点：连接已死。清理跨会话无效的状态
            self._pending_tool_results.clear()
            self._response_active = False
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

        while True:
            pcm = await self.mic.frames.get()
            if self.mic_enabled:  # 键盘模式下丢弃麦克帧（AI 听不到你）
                try:
                    await self.provider.send_audio(pcm)
                except (ConnectionError, OSError):
                    pass  # 断线期丢帧，重连后自动恢复

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

        elif t == "tool.call":
            # Function Calling：执行 → 结果暂存 → 等 response.done 后统一写回+触发二轮
            # （实测：OpenAI 经典 API 经中转网关时，响应进行中写 item 会被静默吞掉，
            #  全部推到 done 后写是唯一稳定顺序，对 Qwen 同样合法）
            name, args = evt["name"], evt.get("arguments", "{}")
            await self.hooks.emit("on_tool_call", name=name, arguments=args)
            self.bus.publish("tool.call", name=name)
            result = await self.tools.dispatch(name, args)
            await self.hooks.emit("on_tool_result", name=name, result=result)
            self.bus.publish("tool.result", name=name, result=result)
            self._pending_tool_results.append((evt["call_id"], result))

        elif t == "response.created":
            self._response_active = True
            self.bus.publish("response.created")

        elif t == "response.done":
            self._response_active = False
            status = evt.get("status")
            self.bus.publish("response.done", status=status, reason=evt.get("reason"))
            await self.hooks.emit("on_turn_end", status=status)
            self._set_state(State.IDLE)
            if self._pending_tool_results:
                pending, self._pending_tool_results = self._pending_tool_results, []
                asyncio.create_task(self._flush_tool_results(pending))

        elif t == "error":
            self.bus.publish("error", error=evt.get("error"))

        elif t in ("session.created", "session.updated"):
            self.bus.publish(t, session=evt.get("session", {}))

    async def _levels(self):
        """10Hz 输出双路电平，UI 律动用。"""
        while True:
            await asyncio.sleep(0.1)
            self.bus.publish("levels",
                             mic=round(self.mic.level, 3) if self.mic else 0.0,
                             speaker=round(self.speaker.level, 3))

    async def _flush_tool_results(self, pending: list[tuple[str, str]]):
        """response.done 后：统一写回工具结果 → 触发二轮推理（带确认重试）。
        背景：中转网关（实测 CometAPI/new-api）会静默丢消息，重试对官方 API 无害。"""
        try:
            for call_id, result in pending:
                print(f"[flush] 写回 {call_id[-6:]} ({len(result)} 字符)", flush=True)
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
    async def close(self):
        if self.mic:
            self.mic.stop()
        self.speaker.stop()
        await self.provider.close()
