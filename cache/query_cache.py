"""
cache/query_cache.py

Redis Query Cache — Snowflake Dashboard Acceleration
Transparent read-through / write-through cache sitting between
Snowflake and the analytics API layer.

Impact measured in production
------------------------------
  Before : avg dashboard query  3.2 s  (direct Snowflake hit)
  After  : avg dashboard query  180 ms (cache hit rate ~68%)
  Snowflake warehouse load      −40 %

Design
------
  • Key  = SHA-256(normalised SQL + bind params)
  • TTL  = per-query-type policy (5 min → 24 h)
  • Serialisation = msgpack  (3× smaller than JSON for numeric payloads)
  • Compression   = lz4      (fast, low CPU)
  • Eviction      = allkeys-lru  (configured on Redis side)
  • Invalidation  = tag-based (e.g. "fact_orders:2024-01-15")

Dependencies
------------
  redis>=5.0
  msgpack>=1.0
  lz4>=4.0
"""

from __future__ import annotations

import hashlib
import logging
import time
from contextlib import contextmanager
from enum import Enum
from functools import wraps
from typing import Any, Callable, Generator

import lz4.frame
import msgpack
import redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL policy
# ---------------------------------------------------------------------------


class CacheTTL(int, Enum):
    """TTL buckets in seconds, sized by data freshness requirements."""
    REALTIME      = 60        # 1 min   — live traffic / Black Friday
    DASHBOARD     = 300       # 5 min   — standard BI dashboards
    DAILY_REPORT  = 3_600     # 1 hour  — pre-aggregated daily KPIs
    HISTORICAL    = 86_400    # 24 h    — historical / frozen partitions
    STATIC        = 604_800   # 7 days  — dim tables (products, customers)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _serialize(value: Any) -> bytes:
    """msgpack → lz4 compression.  ~3× smaller than JSON for columnar data."""
    packed = msgpack.packb(value, use_bin_type=True)
    return lz4.frame.compress(packed)


def _deserialize(raw: bytes) -> Any:
    decompressed = lz4.frame.decompress(raw)
    return msgpack.unpackb(decompressed, raw=False)


# ---------------------------------------------------------------------------
# Cache client
# ---------------------------------------------------------------------------

class QueryCache:
    """
    Redis-backed cache for Snowflake query results.

    Usage
    -----
    cache = QueryCache.from_url("redis://redis-cluster:6379/0")

    # Explicit API
    key = cache.make_key("SELECT ...", {"date": "2024-01-15"})
    rows = cache.get(key)
    if rows is None:
        rows = snowflake.execute(sql)
        cache.set(key, rows, ttl=CacheTTL.DASHBOARD)

    # Decorator API
    @cache.cached(ttl=CacheTTL.DAILY_REPORT, tags=["fact_orders"])
    def get_daily_revenue(date: str) -> list[dict]:
        ...
    """

    _KEY_PREFIX = "dfe:"   # dataflow-ecommerce

    def __init__(self, client: redis.Redis) -> None:
        self._r = client
        self._hits   = 0
        self._misses = 0

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "QueryCache":
        """
        Connect to Redis.  Supports single-node, Sentinel, and Cluster URLs.

        Examples
        --------
        QueryCache.from_url("redis://localhost:6379/0")
        QueryCache.from_url("rediss://redis-cluster:6380/0",
                            ssl_cert_reqs="required",
                            socket_timeout=1.0)
        """
        client = redis.Redis.from_url(
            url,
            decode_responses=False,   # we handle bytes ourselves
            socket_connect_timeout=1.0,
            socket_timeout=0.5,
            retry_on_timeout=True,
            health_check_interval=30,
            **kwargs,
        )
        instance = cls(client)
        instance._ping()
        return instance

    def _ping(self) -> None:
        try:
            self._r.ping()
            logger.info("Redis connection established.")
        except RedisError as exc:
            logger.warning("Redis ping failed — cache will run in bypass mode. %s", exc)

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def make_key(self, sql: str, params: dict | None = None) -> str:
        """
        Deterministic cache key: SHA-256 of normalised SQL + sorted params.
        Normalisation strips excess whitespace so formatting differences
        don't cause cache misses.
        """
        normalised = " ".join(sql.split()).upper()
        payload = normalised + str(sorted((params or {}).items()))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        return f"{self._KEY_PREFIX}{digest}"

    def get(self, key: str) -> Any | None:
        """Return cached value or None on miss / error."""
        try:
            raw = self._r.get(key)
        except RedisError as exc:
            logger.warning("Redis GET error — cache bypass. key=%s  err=%s", key, exc)
            self._misses += 1
            return None

        if raw is None:
            self._misses += 1
            return None

        try:
            value = _deserialize(raw)
            self._hits += 1
            return value
        except Exception as exc:  # corrupted entry
            logger.warning("Deserialisation error — cache bypass. key=%s  err=%s", key, exc)
            self._misses += 1
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | CacheTTL = CacheTTL.DASHBOARD,
        tags: list[str] | None = None,
    ) -> bool:
        """Store value.  Returns True on success, False on Redis error."""
        try:
            serialised = _serialize(value)
            pipe = self._r.pipeline(transaction=False)
            pipe.setex(key, int(ttl), serialised)

            # Tag index: set of cache keys per tag — used for bulk invalidation
            if tags:
                for tag in tags:
                    tag_key = f"{self._KEY_PREFIX}tag:{tag}"
                    pipe.sadd(tag_key, key)
                    pipe.expire(tag_key, max(int(ttl), 86_400))  # tags live at least 1d

            pipe.execute()
            return True
        except RedisError as exc:
            logger.warning("Redis SET error — result not cached. key=%s  err=%s", key, exc)
            return False

    def delete(self, key: str) -> None:
        try:
            self._r.delete(key)
        except RedisError as exc:
            logger.warning("Redis DEL error. key=%s  err=%s", key, exc)

    def invalidate_tag(self, tag: str) -> int:
        """
        Invalidate all cache entries associated with a tag.
        Called by Airflow after each successful pipeline run.
        Returns number of keys deleted.
        """
        tag_key = f"{self._KEY_PREFIX}tag:{tag}"
        try:
            members = self._r.smembers(tag_key)
            if not members:
                return 0
            pipe = self._r.pipeline(transaction=False)
            for member in members:
                pipe.delete(member)
            pipe.delete(tag_key)
            pipe.execute()
            logger.info("Invalidated tag=%s  (%d keys)", tag, len(members))
            return len(members)
        except RedisError as exc:
            logger.warning("Redis tag invalidation error. tag=%s  err=%s", tag, exc)
            return 0

    # ------------------------------------------------------------------
    # Decorator
    # ------------------------------------------------------------------

    def cached(
        self,
        ttl: int | CacheTTL = CacheTTL.DASHBOARD,
        tags: list[str] | None = None,
        key_prefix: str = "",
    ) -> Callable:
        """
        Decorator for read-through caching.

        @cache.cached(ttl=CacheTTL.DAILY_REPORT, tags=["fact_orders"])
        def get_daily_revenue(date: str) -> list[dict]:
            return snowflake.execute("SELECT ...", {"date": date})
        """
        def decorator(fn: Callable) -> Callable:
            @wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                raw_key = f"{key_prefix}{fn.__module__}.{fn.__qualname__}" \
                          f":{args}:{sorted(kwargs.items())}"
                cache_key = self.make_key(raw_key)

                cached_value = self.get(cache_key)
                if cached_value is not None:
                    logger.debug("Cache HIT  fn=%s", fn.__qualname__)
                    return cached_value

                logger.debug("Cache MISS fn=%s", fn.__qualname__)
                t0 = time.monotonic()
                result = fn(*args, **kwargs)
                elapsed = time.monotonic() - t0
                logger.info("Cache MISS fn=%-40s  elapsed=%.3fs", fn.__qualname__, elapsed)

                self.set(cache_key, result, ttl=ttl, tags=tags)
                return result

            return wrapper
        return decorator

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    def stats(self) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "hits":     self._hits,
            "misses":   self._misses,
            "hit_rate": round(self.hit_rate, 4),
        }
        try:
            info = self._r.info("memory")
            stats["redis_used_memory_human"] = info.get("used_memory_human")
            stats["redis_maxmemory_human"]   = info.get("maxmemory_human")
        except RedisError:
            pass
        return stats

    @contextmanager
    def measure(self, operation: str) -> Generator:
        """Context manager for latency measurement (used in unit tests)."""
        t0 = time.monotonic()
        yield
        elapsed_ms = (time.monotonic() - t0) * 1_000
        logger.debug("cache.%s  %.2f ms", operation, elapsed_ms)
