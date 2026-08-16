"""会话层：状态机 + 编排（音频 ↔ Provider ↔ 事件总线），内建延迟计时。

状态机：IDLE → LISTENING → THINKING → SPEAKING → IDLE
打断（barge-in）：SPEAKING 时收到 user.speech_started → 清空播放缓冲 → LISTENING
延迟口径：TTFA = 首个 assistant.audio_delta 到达时刻 − user.speech_stopped 到达时刻
"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum

from .audio import MicCapture, SpeakerPlayback
from .events import EventBus
from .provider import RealtimeProvider
from .tools import ToolRegistry

log = logging.getLogger("session")


class State(Enum):
    IDLE = "idle"
    LISTENING = "listening"
    THINKING = "thinking"
    SPEAKING = "speaking"


DEFAULT_SESSION_CONFIG = {
    "modalities": ["audio", "text"],
    "voice": "longanqian",
    "turn_detection": {"type": "server_vad", "threshold": 0.5, "silence_duration_ms": 800},
    "instructions": (
        "你是一个语音知识助手。回答简洁口语化，像面对面交谈一样自然，"
        "避免 markdown 和长列表；不确定的事情直接说不确定。"
    ),
}


class VoiceSession:
    def __init__(self, provider: RealtimeProvider, bus: EventBus,
                 tools: ToolRegistry | None = None,
                 config: dict | None = None,
                 use_mic: bool = True):
        self.provider = provider
        self.bus = bus
        self.tools = tools or ToolRegistry()
        self.config = {**DEFAULT_SESSION_CONFIG, **(config or {})}
        self.use_mic = use_mic
        self.state = State.IDLE
        self._t_stop: float | None = None       # speech_stopped 到达时刻
        self._first_audio = False               # 本轮是否已见首个音频帧
        self.mic: MicCapture | None = None
        self.speaker = SpeakerPlayback()

    def _set_state(self, s: State):
        if s is not self.state:
            self.state = s
            self.bus.publish("state", state=s.value)

    async def run(self):
        loop = asyncio.get_running_loop()
        await self.provider.connect()
        await self.provider.update_session({**self.config, "tools": self.tools.schemas()})

        self.speaker.start()

        tasks = [self._downlink(), self._levels()]
        if self.use_mic:
            tasks.append(self._start_mic_safely(loop))
        else:
            tasks.append(self._uplink())
        await asyncio.gather(*tasks)

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
        if not self.mic:
            while True:  # 无麦模式（冒烟测试）：只收不发
                await asyncio.sleep(3600)
        else:
            while True:
                pcm = await self.mic.frames.get()
                await self.provider.send_audio(pcm)

    async def _downlink(self):
        try:
            async for evt in self.provider.events():
                await self._handle(evt)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("downlink ended with error")  # M1 不重连，直接暴露
            self.bus.publish("error", error={"type": "connection_lost"})

    async def _handle(self, evt: dict):
        t = evt["type"]

        if t == "user.speech_started":
            if self.state is State.SPEAKING:
                self.speaker.clear()          # 打断：立即静音
                self.bus.publish("interrupted")
            self._set_state(State.LISTENING)
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
            # Function Calling：执行 → 写回 → 触发二轮推理（不阻塞音频管道）
            self.bus.publish("tool.call", name=evt.get("name"))
            result = await self.tools.dispatch(evt["name"], evt.get("arguments", "{}"))
            await self.provider.send_tool_result(evt["call_id"], result)

        elif t == "response.done":
            status = evt.get("status")
            self.bus.publish("response.done", status=status, reason=evt.get("reason"))
            self._set_state(State.IDLE)

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

    async def close(self):
        if self.mic:
            self.mic.stop()
        self.speaker.stop()
        await self.provider.close()
