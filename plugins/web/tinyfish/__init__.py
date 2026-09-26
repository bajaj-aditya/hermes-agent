"""TinyFish web search + fetch plugin — free tier, search/extract only."""
from __future__ import annotations
from plugins.web.tinyfish.provider import TinyFishWebSearchProvider


def register(ctx) -> None:
    ctx.register_web_search_provider(TinyFishWebSearchProvider())
