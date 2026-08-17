"""麦克风硬件自检：采集 5 秒，打印实时 RMS 电平条。

用法：.venv/bin/python scripts/mic_check.py
对着麦克风说话，电平条应明显跳动。全程 0.00 = 硬件/权限问题；
电平很低（<0.05）= 增益不足，可能导致 VAD 不触发（他听不到你）。
"""

import sys
import time

sys.path.insert(0, ".")
from src.core.audio import MIC_RATE, rms

import numpy as np
import sounddevice as sd

DURATION = 5

print(f"采集 {DURATION} 秒（{MIC_RATE}Hz），请对着麦克风说话…\n")
peak = 0.0
frames = []


def cb(indata, n, t, status):
    global peak
    level = rms(indata.tobytes())
    peak = max(peak, level)
    frames.append(level)


with sd.InputStream(samplerate=MIC_RATE, channels=1, dtype="int16",
                    blocksize=int(MIC_RATE * 0.04), callback=cb):
    t0 = time.time()
    i = 0
    while time.time() - t0 < DURATION:
        time.sleep(0.1)
        recent = frames[-3:] or [0.0]
        lv = max(recent)
        bar = "█" * int(lv * 200)
        print(f"\r{lv:5.3f} |{bar:<40}", end="", flush=True)

print(f"\n\n峰值电平: {peak:.3f}")
if peak < 0.01:
    print("❌ 几乎无信号：检查系统设置→隐私→麦克风权限，及输入设备选择")
elif peak < 0.05:
    print("⚠️ 信号偏弱：VAD(threshold=0.5) 可能不触发，考虑调低阈值或靠近麦克风")
else:
    print("✓ 麦克风正常")
