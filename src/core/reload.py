"""Reload：文件监视 → 热加载 → 回调刷新会话（pi 式 reload 体验）。

轮询 mtime（1s），不为它引入 watchdog 依赖。改动防抖 0.5s 后触发回调。
监视范围：src/tools/*.py 与 src/skills/*/SKILL.md。
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Awaitable, Callable

log = logging.getLogger("reload")


def _snapshot(dirs: list[Path]) -> dict[Path, float]:
    snap: dict[Path, float] = {}
    for d in dirs:
        if not d.is_dir():
            continue
        for p in list(d.glob("*.py")) + list(d.glob("*/SKILL.md")):
            try:
                snap[p] = p.stat().st_mtime
            except OSError:
                pass
    return snap


class ReloadManager:
    def __init__(self, watch_dirs: list[str | Path],
                 on_change: Callable[[], Awaitable[None]],
                 interval: float = 1.0):
        self.watch_dirs = [Path(d) for d in watch_dirs]
        self.on_change = on_change
        self.interval = interval
        self._stop = asyncio.Event()

    async def run(self):
        prev = _snapshot(self.watch_dirs)
        while not self._stop.is_set():
            await asyncio.sleep(self.interval)
            cur = _snapshot(self.watch_dirs)
            if cur != prev:
                changed = [p for p in set(cur) | set(prev) if cur.get(p) != prev.get(p)]
                prev = cur
                await asyncio.sleep(0.5)  # 防抖：编辑器可能多次写入
                log.info("检测到变更: %s", [p.name for p in changed])
                try:
                    await self.on_change()
                except Exception:
                    log.exception("reload 回调失败")

    def stop(self):
        self._stop.set()
