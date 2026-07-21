from api.caching import cache_control, make_etag, ttl_for


def test_cacheable_routes_have_ttls():
    assert ttl_for("/dividends/pending") == 30
    assert ttl_for("/health") == 15
    assert ttl_for("/tickers/PETR4/summary") == 30


def test_uncacheable_routes_have_none():
    assert ttl_for("/") is None
    assert ttl_for("/docs") is None
    assert ttl_for("/openapi.json") is None


def test_etag_is_deterministic_and_quoted():
    first = make_etag(b'{"a":1}')
    assert first == make_etag(b'{"a":1}')
    assert first != make_etag(b'{"a":2}')
    assert first.startswith('"') and first.endswith('"')


def test_cache_control_allows_shared_caches():
    value = cache_control(30)
    assert "public" in value
    assert "max-age=30" in value
    assert "stale-while-revalidate" in value
