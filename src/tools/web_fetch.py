"""Web 抓取工具（取数工具）：抓 URL 正文（trafilatura 提取，去导航/广告）。
调研链路：web_search 找链接 → web_fetch 读全文 → 口述摘要 + 落盘。
"""

import asyncio

MAX_CHARS = 6000


def register(registry, ctx):
    @registry.register(
        "web_fetch",
        "抓取指定 URL 的网页正文（自动去除导航广告）。用于读取搜索结果的全文。"
        "返回前 6000 字。",
        parameters={
            "type": "object",
            "properties": {
                "url": {"type": "string", "title": "要抓取的网页 URL（http/https）"},
            },
            "required": ["url"],
        },
        timeout=20,
    )
    async def web_fetch(url: str) -> dict:
        import trafilatura

        def _fetch():
            html = trafilatura.fetch_url(url)
            if not html:
                return None
            return trafilatura.extract(html, include_links=False)

        text = await asyncio.to_thread(_fetch)
        if not text:
            return {"error": f"抓取失败或正文为空：{url}"}
        return {
            "url": url,
            "chars": len(text),
            "content": text[:MAX_CHARS],
            "truncated": len(text) > MAX_CHARS,
        }
