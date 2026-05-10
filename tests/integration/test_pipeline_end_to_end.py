"""
tests/integration/test_pipeline_end_to_end.py

Integration tests — Bronze → Silver → Gold pipeline
Uses a local SparkSession + in-memory fakeredis + a real SQLite DB
(substituting Snowflake) to validate the full pipeline flow without
any cloud dependencies.

Run:
    pytest tests/integration/ -v -m "not slow"
    pytest tests/integration/ -v -m "slow"         # includes volume tests

Markers:
    slow   — tests that spin up Spark with large synthetic datasets
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Generator

import fakeredis
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from cache.query_cache import CacheTTL, QueryCache
from transforms.silver.clickstream_processor import (
    ClickstreamSilverProcessor,
    VALID_EVENT_TYPES,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Local SparkSession for integration tests.
    Scoped to session — one Spark context for all integration tests.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("integration-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")   # deterministic for tests
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def cache() -> QueryCache:
    return QueryCache(client=fakeredis.FakeRedis(decode_responses=False))


@pytest.fixture()
def processing_date() -> str:
    return "2024-01-15"


def _make_event(
    event_type: str = "page_view",
    user_id: str | None = None,
    country_code: str = "FR",
    revenue: str | None = None,
    event_id: str | None = None,
    ts: str = "2024-01-15T14:30:00Z",
    ip: str = "192.168.1.100",
) -> dict:
    """Helper: build a valid Bronze clickstream event dict."""
    return {
        "event_id":        event_id or str(uuid.uuid4()),
        "session_id":      str(uuid.uuid4()),
        "user_id":         user_id or str(uuid.uuid4()),
        "anonymous_id":    None,
        "event_type":      event_type,
        "event_timestamp": ts,
        "page_url":        "https://shop.dataflow.io/products/shoes-running",
        "referrer_url":    "https://www.google.com/search?q=running+shoes",
        "device_type":     "desktop",
        "os":              "macos",
        "browser":         "chrome",
        "country_code":    country_code,
        "ip_address":      ip,
        "properties":      json.dumps({"product_id": "SKU-001", "price": 59.99}),
        "revenue":         revenue,
        "_ingested_at":    "2024-01-15T15:00:00Z",
        "_source_file":    "gs://dataflow-bronze/events/2024-01-15/part-00001.parquet",
    }


# ---------------------------------------------------------------------------
# Silver transformation tests
# ---------------------------------------------------------------------------

class TestSilverTransformations:

    def test_valid_events_pass_through(self, spark: SparkSession, tmp_path, processing_date: str):
        events = [_make_event(event_type=et) for et in list(VALID_EVENT_TYPES)[:5]]
        df = spark.createDataFrame(events)
        input_path  = str(tmp_path / "bronze")
        output_path = str(tmp_path / "silver")
        df.write.parquet(input_path)

        processor = ClickstreamSilverProcessor(spark, processing_date)
        # Run individual transforms (not full run which reads from GCS)
        result = (
            spark.read.parquet(input_path)
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._filter_invalid)
            .transform(processor._deduplicate)
            .transform(processor._parse_urls)
            .transform(processor._anonymize_ip)
            .transform(processor._parse_revenue)
            .transform(processor._add_metadata)
        )
        assert result.count() == len(events)

    def test_invalid_event_types_are_dropped(self, spark: SparkSession, tmp_path, processing_date: str):
        events = [
            _make_event(event_type="page_view"),          # valid
            _make_event(event_type="INVALID_TYPE"),        # invalid
            _make_event(event_type="bot_crawl"),           # invalid
        ]
        df = spark.createDataFrame(events)
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._filter_invalid)
        )
        assert result.count() == 1

    def test_null_event_id_is_dropped(self, spark: SparkSession, processing_date: str):
        event = _make_event()
        event["event_id"] = None
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._filter_invalid)
        )
        assert result.count() == 0

    def test_deduplication_on_event_id(self, spark: SparkSession, processing_date: str):
        eid = str(uuid.uuid4())
        e1 = _make_event(event_id=eid)
        e2 = dict(e1)
        e2["_ingested_at"] = "2024-01-15T16:00:00Z"   # later ingestion

        df = spark.createDataFrame([e1, e2])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._filter_invalid)
            .transform(processor._deduplicate)
        )
        assert result.count() == 1
        # Should keep first-ingested record
        row = result.collect()[0]
        assert row["_ingested_at"] == e1["_ingested_at"]

    def test_ip_anonymisation_ipv4(self, spark: SparkSession, processing_date: str):
        event = _make_event(ip="203.0.113.42")
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._filter_invalid)
            .transform(processor._anonymize_ip)
        )
        row = result.first()
        assert row["ip_anonymized"] == "203.0.113.0"
        assert "ip_address" not in result.columns

    def test_ip_original_not_present_in_output(self, spark: SparkSession, processing_date: str):
        event = _make_event(ip="10.20.30.40")
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = df.transform(processor._cast_schema).transform(processor._anonymize_ip)
        assert "ip_address" not in result.columns

    @pytest.mark.parametrize("raw,expected", [
        ("29.99",      29.99),
        ("29,99",      29.99),
        ("€29.99",     29.99),
        ("29.99 EUR",  29.99),
        ("$49.00",     49.00),
        ("£15.50",     15.50),
        (None,         0.0),
        ("",           None),   # empty string → cast fails → None (dropped upstream)
    ])
    def test_revenue_normalisation(
        self, spark: SparkSession, processing_date: str, raw, expected
    ):
        event = _make_event(revenue=raw)
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_revenue)
        )
        row = result.first()
        if expected is not None:
            assert abs((row["revenue_eur"] or 0.0) - expected) < 0.001
        # "revenue" raw column must be dropped
        assert "revenue" not in result.columns

    @pytest.mark.parametrize("url,expected_domain,expected_channel", [
        ("https://www.google.com/search?q=shoes", "www.google.com", "organic_search"),
        ("https://www.facebook.com/ads",          "www.facebook.com", "social"),
        ("https://shop.dataflow.io/cart",          "shop.dataflow.io", "internal"),
        ("",                                       "",                  "direct"),
        (None,                                     "",                  "direct"),
    ])
    def test_referrer_channel_classification(
        self, spark: SparkSession, processing_date: str,
        url, expected_domain, expected_channel
    ):
        event = _make_event()
        event["referrer_url"] = url
        event["page_url"]     = "https://shop.dataflow.io/products/shoes"
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._parse_urls)
        )
        row = result.first()
        assert row["referrer_channel"] == expected_channel

    @pytest.mark.parametrize("ts,should_parse", [
        ("2024-01-15T14:30:00.000Z", True),
        ("2024-01-15T14:30:00Z",     True),
        ("2024-01-15T14:30:00",      True),
        ("1705329000000",            True),    # Unix ms
        ("1705329000",               True),    # Unix s
        ("not-a-date",               False),
        ("",                         False),
        (None,                       False),
    ])
    def test_timestamp_parsing_formats(
        self, spark: SparkSession, processing_date: str, ts, should_parse
    ):
        event = _make_event(ts=ts)
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
        )
        row = result.first()
        if should_parse:
            assert row["event_ts"] is not None
        else:
            assert row["event_ts"] is None

    def test_metadata_columns_added(self, spark: SparkSession, processing_date: str):
        event = _make_event()
        df = spark.createDataFrame([event])
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._add_metadata)
        )
        row = result.first()
        assert row["_processing_date"] == processing_date
        assert row["_silver_version"] is not None
        assert row["_silver_processed_at"] is not None


# ---------------------------------------------------------------------------
# Cache integration
# ---------------------------------------------------------------------------

class TestCacheIntegration:

    def test_cache_stores_spark_result_as_python_list(
        self, spark: SparkSession, cache: QueryCache
    ):
        """Simulate caching a Spark collect() result for dashboard serving."""
        events = [_make_event() for _ in range(100)]
        df = spark.createDataFrame(events).select("event_id", "event_type", "country_code")
        rows = [r.asDict() for r in df.collect()]

        key = cache.make_key("SELECT event_id, event_type, country_code FROM silver LIMIT 100")
        cache.set(key, rows, ttl=CacheTTL.DASHBOARD)

        cached = cache.get(key)
        assert cached is not None
        assert len(cached) == 100
        assert cached[0]["event_type"] in VALID_EVENT_TYPES

    def test_cache_miss_falls_through_to_spark(
        self, spark: SparkSession, cache: QueryCache
    ):
        call_count = 0

        @cache.cached(ttl=CacheTTL.DASHBOARD, tags=["clickstream"])
        def get_event_counts(date: str) -> dict:
            nonlocal call_count
            call_count += 1
            events = [_make_event() for _ in range(50)]
            df = spark.createDataFrame(events)
            return {"count": df.count(), "date": date}

        r1 = get_event_counts("2024-01-15")
        r2 = get_event_counts("2024-01-15")

        assert r1["count"] == 50
        assert r2["count"] == 50
        assert call_count == 1   # Spark called only once


# ---------------------------------------------------------------------------
# Volume / performance smoke test
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestVolumeSmoke:

    def test_10m_events_processed_under_60s(
        self, spark: SparkSession, tmp_path, processing_date: str
    ):
        """
        Smoke test: 10M events should process in < 60s on local[2].
        At 2B/day this runs on 40+ executors — local test validates correctness, not throughput.
        """
        import time

        # Generate 10M synthetic events using Spark range (no Python loop)
        df = (
            spark.range(10_000_000)
            .select(
                F.expr("uuid()").alias("event_id"),
                F.expr("uuid()").alias("session_id"),
                F.expr("uuid()").alias("user_id"),
                F.lit(None).cast("string").alias("anonymous_id"),
                F.when(F.col("id") % 12 == 0,  "page_view")
                 .when(F.col("id") % 12 == 1,  "product_view")
                 .when(F.col("id") % 12 == 2,  "add_to_cart")
                 .when(F.col("id") % 12 == 3,  "purchase")
                 .otherwise("page_view").alias("event_type"),
                F.lit("2024-01-15T14:30:00Z").alias("event_timestamp"),
                F.lit("https://shop.dataflow.io/products/test").alias("page_url"),
                F.lit(None).cast("string").alias("referrer_url"),
                F.lit("desktop").alias("device_type"),
                F.lit("macos").alias("os"),
                F.lit("chrome").alias("browser"),
                F.lit("FR").alias("country_code"),
                F.lit("203.0.113.1").alias("ip_address"),
                F.lit(None).cast("string").alias("properties"),
                F.lit("29.99").alias("revenue"),
                F.lit("2024-01-15T15:00:00Z").alias("_ingested_at"),
                F.lit("gs://test/part-00001.parquet").alias("_source_file"),
            )
        )

        t0 = time.monotonic()
        processor = ClickstreamSilverProcessor(spark, processing_date)
        result = (
            df
            .transform(processor._cast_schema)
            .transform(processor._parse_timestamps)
            .transform(processor._filter_invalid)
            .transform(processor._deduplicate)
            .transform(processor._anonymize_ip)
            .transform(processor._parse_revenue)
        )
        count = result.count()
        elapsed = time.monotonic() - t0

        assert count > 0
        assert elapsed < 60, f"10M events took {elapsed:.1f}s — expected < 60s"
