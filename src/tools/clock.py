"""时间工具：M1.5 验证工具——零外部依赖，跑通 Function Calling 全链路。"""

from datetime import datetime


def register(registry, ctx):
    @registry.register(
        "get_current_time",
        "获取当前的日期、时间和星期几。用户问时间、日期、今天星期几时必须调用，不要猜。",
    )
    async def get_current_time() -> dict:
        now = datetime.now().astimezone()
        weekdays = "一二三四五六日"
        return {
            "datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
            "weekday": f"星期{weekdays[now.weekday()]}",
            "timezone": str(now.tzname()),
        }
