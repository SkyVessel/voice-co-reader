"""音频层：麦克风采集（16kHz PCM16 mono）+ 扬声器播放（24kHz PCM16 mono，可打断清空）。

同时输出 RMS 电平事件（mic.level / speaker.level）——M2 语音律动组件的数据源。
"""

from __future__ import annotations

import asyncio
import logging
import queue
import threading

import numpy as np
import sounddevice as sd

log = logging.getLogger("audio")

MIC_RATE = 16000       # Qwen realtime 输入固定 16kHz
SPEAKER_RATE = 24000   # Qwen realtime 输出固定 24kHz
FRAME_MS = 40          # 官方建议每 20~40ms 一帧


def rms(pcm: bytes) -> float:
    """PCM16 字节的 RMS 电平，归一化到 0~1。"""
    if not pcm:
        return 0.0
    samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32)
    return float(np.sqrt(np.mean(samples**2)) / 32768.0)


class MicCapture:
    """回调线程把 PCM 帧推进 asyncio 队列。"""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.frames: asyncio.Queue[bytes] = asyncio.Queue()
        self.level = 0.0
        self._stream = sd.InputStream(
            samplerate=MIC_RATE, channels=1, dtype="int16",
            blocksize=int(MIC_RATE * FRAME_MS / 1000),
            callback=self._on_frame,
        )

    def _on_frame(self, indata, frames, time_info, status):
        if status:
            log.warning("mic status: %s", status)
        pcm = indata.tobytes()
        self.level = rms(pcm)
        self.loop.call_soon_threadsafe(self.frames.put_nowait, pcm)

    def start(self):
        self._stream.start()

    def stop(self):
        self._stream.stop()
        self._stream.close()


class SpeakerPlayback:
    """独立播放线程；clear() 立即清空待发缓冲（barge-in）。"""

    def __init__(self):
        self._q: queue.Queue[bytes | None] = queue.Queue()
        self.level = 0.0
        self._stream = sd.RawOutputStream(samplerate=SPEAKER_RATE, channels=1, dtype="int16")
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while True:
            chunk = self._q.get()
            if chunk is None:  # 停止信号
                break
            self.level = rms(chunk)
            try:
                self._stream.write(chunk)
            except Exception as e:  # 关闭竞态/设备抖动不致命
                log.warning("playback write failed: %s", e)
                break

    def start(self):
        self._stream.start()
        self._thread.start()

    def enqueue(self, pcm: bytes):
        self._q.put(pcm)

    def clear(self):
        """打断：丢弃所有待播放音频。"""
        while not self._q.empty():
            try:
                self._q.get_nowait()
            except queue.Empty:
                break
        self.level = 0.0

    def stop(self):
        self._q.put(None)
        if self._thread.is_alive() or self._thread.ident is not None:
            self._thread.join(timeout=1)
        try:
            self._stream.stop()
            self._stream.close()
        except Exception:
            pass
