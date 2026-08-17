"""委派工具：前台（realtime）向后台主力模型派活。

两个工具都是"点单即走"：发布总线事件后立刻返回，前台不等待、继续陪用户对话；
后台结果由 delegate 服务注入会话，前台收到后再播报/讲解（异步委派）。
"""

from __future__ import annotations


def register(registry, ctx):
    @registry.register(
        "deep_think",
        "把需要深度推理、复杂分析、多步查证或你没把握的问题，委派给后台主力模型。"
        "调用后立即返回（异步），你继续和用户聊；结果稍后自动回到对话里，你再向用户汇报要点。"
        "长任务/复杂任务也可以交给它做任务编排。question 说清楚任务，context 补充相关背景。",
        parameters={
            "type": "object",
            "properties": {
                "question": {"type": "string", "title": "交办给主力模型的任务描述"},
                "context": {"type": "string", "title": "相关背景（可选），帮助它理解上下文"},
            },
            "required": ["question"],
        },
        timeout=3,
    )
    async def deep_think(ctx, question: str, context: str = "") -> dict:
        ctx.bus.publish("delegate.think", question=question, context=context)
        return {"ok": True, "status": "已受理，主力模型工作中，结果稍后自动回到对话"}

    @registry.register(
        "request_note",
        "向秘书点单写笔记（替代自己写）。讲到值得留存的结论、数据、定义、来源时调用："
        "你只点单，秘书记录对话原文并让主力模型写出详实笔记；卡片上屏后你会收到通知，"
        "那时再针对卡片向用户讲解一两句。topic 是主题，points 是你希望笔记覆盖的要点。",
        parameters={
            "type": "object",
            "properties": {
                "topic": {"type": "string", "title": "笔记主题"},
                "points": {"type": "string", "title": "希望覆盖的要点（可选）"},
            },
            "required": ["topic"],
        },
        timeout=3,
    )
    async def request_note(ctx, topic: str, points: str = "") -> dict:
        ctx.bus.publish("delegate.note", topic=topic, points=points)
        return {"ok": True, "status": "已点单，秘书撰写中，上屏后会通知你"}
