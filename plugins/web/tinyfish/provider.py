"""TinyFish (https://tinyfish.ai) web search + page fetch -- free tier only.

Env: ``TINYFISH_API_KEY``.

Free tier, by construction rather than by policy
------------------------------------------------
TinyFish sells four products. Two are free and two draw on a prepaid Wallet:

    Search   free                    Agent    $0.016 / step
    Fetch    free                    Browser  $0.002 / minute

This provider registers only ``search`` and ``extract``. The Hermes web-provider
interface has no verb that could reach an Agent or a Browser, so no prompt, no
config edit and no model decision can make this spend money. Browser work stays
on Browser Use Cloud.

Verified on the box 2026-09-25: 4 searches + 3 fetches drew $0.0000 against a
wallet that *publishes* contract rates for both (``$0.005/query``,
``$0.001/url``). The rates exist in the billing schema but are not charged.
The wallet also reports ``auto_reload: unconfigured`` -- no card, so even a
hypothetical charge is capped at the standing balance.

What "free tier" actually costs you: rate limits
------------------------------------------------
Free means hard ceilings -- search 30/min and 500/hour, fetch 150 urls/min and
1,000 urls/day, at most 10 URLs per request. Because ``web.search_backend`` is
pinned to this provider, a 429 would otherwise dead-end with no fallback: the
pin bypasses the registry's preference walk entirely.

So throttling is handled *here*, by failing over into the keyless ring
(exa / parallel / firecrawl / keenable), which is also free and needs no key.
The caller never sees a rate limit and never has to choose a backend -- the same
shape as ``bu.run()`` walking its model list. Non-throttle errors are returned
as-is: a malformed query fails everywhere, and retrying it elsewhere is waste.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from plugins.web._common import (
    SEARCH_LIMIT_CAP, BaseWebSearchProvider, document, http_status_detail, keyless_extract, keyless_search,
    page_error, provider_env, run_extract, run_search, search_fail, search_ok, setup_schema, web_hit,
)

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://api.search.tinyfish.ai/"
_FETCH_URL = "https://api.fetch.tinyfish.ai"
_FETCH_BATCH = 10  # vendor cap: a POST carrying more than 10 URLs is rejected outright
_PER_URL_TIMEOUT_MS = 45000  # their backend hard-stops at 110s; stay inside our own read timeout

# 429 is the documented free-tier ceiling; 5xx is TinyFish being down. Both mean
# "ask someone else", as opposed to a 4xx that means "this request is wrong".
_FAILOVER_STATUSES = frozenset({429, 500, 502, 503, 504})


def _headers(api_key: str) -> Dict[str, str]:
    return {"X-API-Key": api_key}


class TinyFishWebSearchProvider(BaseWebSearchProvider):
    """TinyFish search + fetch. Free endpoints only; throttling falls through to keyless."""

    NAME = "tinyfish"
    DISPLAY_NAME = "TinyFish"
    KEY_ENV = "TINYFISH_API_KEY"
    EXTRACT = True
    # Not a keyless-ring member: free of charge, but still key-gated. Leaving this
    # False keeps the keyless walk from ever routing here on a keyless install.
    KEYLESS = False

    def search(self, query: str, limit: int = 5) -> Dict[str, Any]:
        def _body() -> Dict[str, Any]:
            api_key = provider_env(self.KEY_ENV)
            if not api_key:
                return search_fail("TINYFISH_API_KEY is not set")
            import requests
            logger.info("TinyFish search: '%s' (limit=%d)", query, limit)
            response = requests.get(
                _SEARCH_URL, params={"query": query},
                headers=_headers(api_key), timeout=30,
            )
            if response.status_code in _FAILOVER_STATUSES:
                logger.info("TinyFish search HTTP %s -- failing over to the keyless ring",
                            response.status_code)
                return keyless_search(self.DISPLAY_NAME, self.NAME, query, limit, logger)
            if response.status_code >= 400:
                return search_fail(f"TinyFish search failed: {http_status_detail(response)}")
            # The endpoint pages rather than taking a max_results, so cap client-side.
            rows = (response.json().get("results") or [])[:min(max(1, int(limit)), SEARCH_LIMIT_CAP)]
            return search_ok([
                web_hit(r.get("url") or "", r.get("title") or "", r.get("snippet") or "",
                        r.get("position") or i + 1)
                for i, r in enumerate(rows)
            ])

        return run_search(self.DISPLAY_NAME, logger, _body, verbatim_value_error=False)

    def extract(self, urls: List[str], **kwargs: Any) -> List[Dict[str, Any]]:
        def _body() -> List[Dict[str, Any]]:
            api_key = provider_env(self.KEY_ENV)
            if not api_key:
                return [page_error(u, "TINYFISH_API_KEY is not set") for u in urls]
            import requests
            logger.info("TinyFish extract: %d URL(s)", len(urls))
            by_url: Dict[str, Dict[str, Any]] = {}
            for start in range(0, len(urls), _FETCH_BATCH):
                batch = urls[start:start + _FETCH_BATCH]
                try:
                    response = requests.post(
                        _FETCH_URL,
                        json={"urls": batch, "format": "markdown",
                              "per_url_timeout_ms": _PER_URL_TIMEOUT_MS},
                        headers=_headers(api_key), timeout=120,
                    )
                    if response.status_code in _FAILOVER_STATUSES:
                        logger.info("TinyFish fetch HTTP %s -- failing over to the keyless ring",
                                    response.status_code)
                        for doc in keyless_extract(self.DISPLAY_NAME, self.NAME, batch, logger):
                            by_url[doc.get("url") or ""] = doc
                        continue
                    if response.status_code >= 400:
                        raise ValueError(http_status_detail(response))
                    payload = response.json()
                except Exception as exc:  # noqa: BLE001 -- whole batch died; fail only its URLs
                    for url in batch:
                        by_url[url] = page_error(url, f"TinyFish extract failed: {exc}")
                    continue
                for row in payload.get("results") or []:
                    source = row.get("url") or ""
                    by_url[source] = document(
                        row.get("final_url") or source, row.get("title") or "",
                        row.get("text") or "", source_url=source,
                    )
                # Per-URL failures ride along in errors[] on an HTTP 200, so a batch can be
                # half-good; anything in neither list is caught by the fill-in below.
                for row in payload.get("errors") or []:
                    source = row.get("url") or ""
                    detail = row.get("error") or "unknown error"
                    status = row.get("status")
                    by_url[source] = page_error(
                        source, f"TinyFish extract failed: {detail}"
                        + (f" (HTTP {status})" if status else ""))
            return [by_url.get(u) or page_error(u, "TinyFish returned no result for this URL")
                    for u in urls]

        return run_extract(self.DISPLAY_NAME, logger, urls, _body, verbatim_value_error=False)

    def get_setup_schema(self) -> Dict[str, Any]:
        return setup_schema(
            "TinyFish", "free · key required",
            "Web search + page fetch on TinyFish's free tier (30 searches/min, 1,000 fetches/day); "
            "throttling fails over to the keyless ring. Cannot reach TinyFish's paid Agent or "
            "Browser products -- neither is registered.",
            self.KEY_ENV, "TinyFish API key", "https://agent.tinyfish.ai/api-keys",
        )
