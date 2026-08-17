"""Web 搜索工具（取数工具）。后端：Exa 免 key MCP（主）→ DuckDuckGo（兜底）。

Exa 免 key 通道：https://mcp.exa.ai/mcp（JSON-RPC tools/call web_search_exa，SSE 返回），
无需注册、有速率限制；失败时自动降级 ddgs，工具接口对模型不变。
"""

from __future__ import annotations

import asyncio
import json
import re
import urllib.request

EXA_MCP = "https://mcp.exa.ai/mcp"


def _exa_search(query: str, num: int) -> list[dict]:
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "web_search_exa",
                   "arguments": {"query": query, "numResults": num}},
    }).encode()
    req = urllib.request.Request(EXA_MCP, data=body, headers={
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})  # 无 UA 会被网关 403
    raw = urllib.request.urlopen(req, timeout=12).read().decode()
    data_line = next((ln[6:] for ln in raw.splitlines() if ln.startswith("data: ")), None)
    if not data_line:
        raise RuntimeError("exa 返回无 data 行")
    payload = json.loads(data_line)
    if payload.get("error"):
        raise RuntimeError(f"exa 错误: {payload['error']}")
    text = payload["result"]["content"][0]["text"]
    results = []
    for block in re.split(r"\n\s*\n", text):
        t = re.search(r"^Title:\s*(.+)$", block, re.M)
        u = re.search(r"^URL:\s*(.+)$", block, re.M)
        if t and u:
            snippet = block.split("Highlights:", 1)[-1].strip() if "Highlights:" in block else ""
            results.append({"title": t.group(1).strip(), "url": u.group(1).strip(),
                            "snippet": re.sub(r"\s+", " ", snippet)[:300]})
    if not results:
        raise RuntimeError("exa 解析无结果")
    return results


def _ddg_search(query: str, num: int) -> list[dict]:
    from ddgs import DDGS
    with DDGS() as d:
        return [{"title": r.get("title", ""), "url": r.get("href", ""),
                 "snippet": r.get("body", "")[:300]}
                for r in d.text(query, max_results=num)]


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
        num = min(max_results, 10)
        try:
            results = await asyncio.to_thread(_exa_search, query, num)
            backend = "exa"
        except Exception:
            results = await asyncio.to_thread(_ddg_search, query, num)
            backend = "ddg-fallback"
        return {"query": query, "backend": backend, "results": results}
