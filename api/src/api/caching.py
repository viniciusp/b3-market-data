"""HTTP caching for the read-only endpoints.

Every response is broadcast data (identical for all users) that changes at most
every few minutes, so ETag + Cache-Control let browsers revalidate for free and
let any HTTP cache or CDN in front collapse arbitrary user traffic into a few
origin hits per TTL window.
"""

import hashlib

# Seconds of freshness per route; None (unlisted) means no caching headers.
ROUTE_TTL = {
    "/dividends/pending": 30,
    "/health": 15,
}
TICKERS_PREFIX = "/tickers/"
TICKERS_TTL = 30
STALE_WHILE_REVALIDATE = 300


def ttl_for(path: str) -> int | None:
    if path in ROUTE_TTL:
        return ROUTE_TTL[path]
    if path.startswith(TICKERS_PREFIX):
        return TICKERS_TTL
    return None


def make_etag(body: bytes) -> str:
    return f'"{hashlib.sha256(body).hexdigest()[:16]}"'


def cache_control(ttl: int) -> str:
    return f"public, max-age={ttl}, stale-while-revalidate={STALE_WHILE_REVALIDATE}"
