"""文件工具共享的访问范围配置。

产品定位是个人桌面助理 → 默认放开到整个用户主目录（Desktop/Documents 都可用）。
AGENT_FS_ROOT 环境变量可收紧范围（如限定项目目录）。
逃逸保护仍生效：ROOT 之外（如 /etc、/System）会被拦截。
"""

import os
from pathlib import Path

ROOT = Path(os.environ.get("AGENT_FS_ROOT", str(Path.home()))).expanduser().resolve()


def resolve(path: str) -> Path:
    p = Path(path).expanduser()
    p = (ROOT / p).resolve() if not p.is_absolute() else p.resolve()
    if not (p == ROOT or str(p).startswith(str(ROOT) + os.sep)):
        raise ValueError(f"路径越界：{path}（只允许 {ROOT} 内）")
    return p
