"""
tests/unit/test_query_cache.py

Unit tests for the Redis query cache layer.
Uses fakeredis — no real Redis instance needed.

Run: pytest tests/unit/test_query_cache.py -v
"""

from __future__ import annotations

import time
from unittest.mock import patch

import fakeredis
import pytest

from cache.query_cache import CacheTTL, QueryCache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cache() -> QueryCache:
    """QueryCache backed by fakeredis (in-process, no network)."""
    fake_client = fakeredis.FakeRedis(decode_responses=False)
    return QueryCache(client=fake_client)


SAMPLE_ROWS = [
    {"order_id": "ord-001", "revenue_eur": 99.99,  "country": "FR"},
    {"order_id": "ord-002", "revenue_eur": 149.00, "country": "DE"},
    {"order_id": "ord-003", "revenue_eur": 29.50,  "country": "ES"},
]


# ---------------------------------------------------------------------------
# make_key
# ---------------------------------------------------------------------------

class TestMakeKey:

    def test_same_sql_same_key(self, cache):
        k1 = cache.make_key("SELECT * FROM fact_orders WHERE date = :d", {"d": "2024-01-15"})
        k2 = cache.make_key("SELECT * FROM fact_orders WHERE date = :d", {"d": "2024-01-15"})
        assert k1 == k2

    def test_different_params_different_key(self, cache):
        k1 = cache.make_key("SELECT * FROM fact_orders", {"date": "2024-01-15"})
        k2 = cache.make_key("SELECT * FROM fact_orders", {"date": "2024-01-16"})
        assert k1 != k2

    def test_whitespace_normalisation(self, cache):
        k1 = cache.make_key("SELECT   *   FROM  fact_orders")
        k2 = cache.make_key("SELECT * FROM fact_orders")
        assert k1 == k2

    def test_key_has_prefix(self, cache):
        key = cache.make_key("SELECT 1")
        assert key.startswith("dfe:")


# ---------------------------------------------------------------------------
# get / set
# ---------------------------------------------------------------------------

class TestGetSet:

    def test_miss_returns_none(self, cache):
        assert cache.get("nonexistent-key") is None

    def test_set_then_get_returns_value(self, cache):
        key = cache.make_key("SELECT revenue FROM fact_orders")
        cache.set(key, SAMPLE_ROWS, ttl=CacheTTL.DASHBOARD)
        result = cache.get(key)
        assert result == SAMPLE_ROWS

    def test_nested_dicts_preserved(self, cache):
        data = {"meta": {"total": 3, "page": 1}, "rows": SAMPLE_ROWS}
        key = cache.make_key("complex-query")
        cache.set(key, data)
        assert cache.get(key) == data

    def test_large_payload(self, cache):
        """100 000-row result should survive serialise/deserialise round-trip."""
        big = [{"id": i, "val": i * 1.5, "label": f"item-{i}"} for i in range(100_000)]
        key = cache.make_key("big-query")
        cache.set(key, big)
        assert cache.get(key) == big

    def test_set_returns_true_on_success(self, cache):
        key = cache.make_key("test-set-return")
        assert cache.set(key, {"ok": True}) is True


# ---------------------------------------------------------------------------
# TTL expiry
# ---------------------------------------------------------------------------

class TestTTL:

    def test_expired_key_returns_none(self, cache):
        key = cache.make_key("expiring-query")
        cache.set(key, {"rows": 42}, ttl=1)   # 1 second
        time.sleep(1.1)
        assert cache.get(key) is None

    def test_not_yet_expired_returns_value(self, cache):
        key = cache.make_key("not-expired")
        cache.set(key, {"rows": 42}, ttl=10)
        assert cache.get(key) is not None


# ---------------------------------------------------------------------------
# Tag-based invalidation
# ---------------------------------------------------------------------------

class TestTagInvalidation:

    def test_invalidate_tag_deletes_associated_keys(self, cache):
        keys = [cache.make_key(f"query-{i}") for i in range(5)]
        for key in keys:
            cache.set(key, SAMPLE_ROWS, tags=["fact_orders:2024-01-15"])

        deleted = cache.invalidate_tag("fact_orders:2024-01-15")
        assert deleted == 5
        for key in keys:
            assert cache.get(key) is None

    def test_invalidate_nonexistent_tag_returns_zero(self, cache):
        assert cache.invalidate_tag("ghost-tag") == 0

    def test_other_tags_not_affected(self, cache):
        k1 = cache.make_key("q-tag-a")
        k2 = cache.make_key("q-tag-b")
        cache.set(k1, {"tag": "a"}, tags=["tag_a"])
        cache.set(k2, {"tag": "b"}, tags=["tag_b"])

        cache.invalidate_tag("tag_a")

        assert cache.get(k1) is None        # invalidated
        assert cache.get(k2) == {"tag": "b"}  # untouched


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

class TestCachedDecorator:

    def test_function_called_only_once_on_two_invocations(self, cache):
        call_count = 0

        @cache.cached(ttl=CacheTTL.DASHBOARD)
        def expensive_query(date: str) -> list[dict]:
            nonlocal call_count
            call_count += 1
            return SAMPLE_ROWS

        result1 = expensive_query("2024-01-15")
        result2 = expensive_query("2024-01-15")
        assert result1 == result2
        assert call_count == 1

    def test_different_args_different_cache_entries(self, cache):
        call_count = 0

        @cache.cached(ttl=CacheTTL.DASHBOARD)
        def get_revenue(date: str) -> float:
            nonlocal call_count
            call_count += 1
            return 1234.56

        get_revenue("2024-01-15")
        get_revenue("2024-01-16")
        assert call_count == 2


# ---------------------------------------------------------------------------
# Hit rate statistics
# ---------------------------------------------------------------------------

class TestStats:

    def test_hit_rate_zero_on_fresh_cache(self, cache):
        assert cache.hit_rate == 0.0

    def test_hit_rate_reflects_hits_and_misses(self, cache):
        key = cache.make_key("stats-test")

        cache.get("miss-1")          # miss
        cache.get("miss-2")          # miss
        cache.set(key, {"v": 1})
        cache.get(key)               # hit
        cache.get(key)               # hit

        # 2 hits / 4 total = 0.5
        assert cache.hit_rate == pytest.approx(0.5, abs=0.01)

    def test_stats_dict_keys_present(self, cache):
        s = cache.stats()
        assert "hits"     in s
        assert "misses"   in s
        assert "hit_rate" in s
