"""Web 搜索工具（取数工具）。当前后端：DuckDuckGo（零配置）。
M4 正式选型（Tavily/Exa/Brave）后换后端，工具接口不变。
"""

import asyncio


def register(registry, ctx):
    @registry.register(
        "web_search",
        "联网搜索最新信息。用户问时事、新闻、价格、你不确定的事实时调用。"
        "返回标题/摘要/链接列表。需要看页面全文时再用 web_fetch 抓链接。",
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "title": "搜索关键词"},
                "max_results": {"type": "number", "title": "返回条数，默认 5，最多 10"},
            },
            "required": ["query"],
        },
        timeout=15,
    )
    async def web_search(query: str, max_results: int = 5) -> dict:
        from ddgs import DDGS

        def _search():
            with DDGS() as d:
                return list(d.text(query, max_results=min(max_results, 10)))

        results = await asyncio.to_thread(_search)
        return {
            "query": query,
            "results": [
                {"title": r.get("title", ""), "snippet": r.get("body", "")[:300],
                 "url": r.get("href", "")}
                for r in results
            ],
        }
