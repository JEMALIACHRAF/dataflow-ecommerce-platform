"""
tests/unit/test_clickstream_processor.py

Unit tests for the Silver clickstream processor.
Uses a local SparkSession (no Databricks / GCS required).

Run: pytest tests/unit/test_clickstream_processor.py -v --tb=short
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from transforms.silver.clickstream_processor import (
    VALID_EVENT_TYPES,
    ClickstreamSilverProcessor,
    create_spark_session,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-clickstream-silver")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def processor(spark: SparkSession) -> ClickstreamSilverProcessor:
    return ClickstreamSilverProcessor(spark, processing_date="2024-01-15")


def _make_event(**kwargs) -> dict:
    """Return a minimal valid Bronze event dict, overridable via kwargs."""
    defaults = {
        "event_id":        "evt-001",
        "session_id":      "sess-abc",
        "user_id":         "usr-123",
        "anonymous_id":    None,
        "event_type":      "page_view",
        "event_timestamp": "2024-01-15T10:30:00Z",
        "page_url":        "https://shop.example.com/products/sneakers",
        "referrer_url":    "https://www.google.com/search?q=sneakers",
        "device_type":     "desktop",
        "os":              "macos",
        "browser":         "chrome",
        "country_code":    "FR",
        "ip_address":      "192.168.1.42",
        "properties":      '{"product_id": "prod-999"}',
        "revenue":         None,
        "_ingested_at":    "2024-01-15T11:00:00Z",
        "_source_file":    "gs://dataflow-bronze/events/2024-01-15/part-00000.parquet",
    }
    return {**defaults, **kwargs}


# ---------------------------------------------------------------------------
# _cast_schema
# ---------------------------------------------------------------------------

class TestCastSchema:

    def test_event_type_lowercased(self, spark, processor):
        df = spark.createDataFrame([_make_event(event_type="PAGE_VIEW")])
        result = processor._cast_schema(df).select("event_type").first()
        assert result["event_type"] == "page_view"

    def test_country_code_uppercased(self, spark, processor):
        df = spark.createDataFrame([_make_event(country_code="fr")])
        result = processor._cast_schema(df).select("country_code").first()
        assert result["country_code"] == "FR"

    def test_whitespace_trimmed(self, spark, processor):
        df = spark.createDataFrame([_make_event(event_type="  add_to_cart  ")])
        result = processor._cast_schema(df).select("event_type").first()
        assert result["event_type"] == "add_to_cart"


# ---------------------------------------------------------------------------
# _parse_timestamps
# ---------------------------------------------------------------------------

class TestParseTimestamps:

    @pytest.mark.parametrize("raw_ts, expected_date", [
        ("2024-01-15T10:30:00Z",         "2024-01-15"),
        ("2024-01-15T10:30:00.000+0200",  "2024-01-15"),
        ("2024-01-15T10:30:00",           "2024-01-15"),
        ("1705312200000",                 "2024-01-15"),   # Unix ms
        ("1705312200",                    "2024-01-15"),   # Unix s
    ])
    def test_timestamp_formats(self, spark, processor, raw_ts, expected_date):
        df = spark.createDataFrame([_make_event(event_timestamp=raw_ts)])
        df = processor._cast_schema(df)
        result = processor._parse_timestamps(df).select("event_date").first()
        assert str(result["event_date"]) == expected_date

    def test_invalid_timestamp_produces_null(self, spark, processor):
        df = spark.createDataFrame([_make_event(event_timestamp="not-a-date")])
        df = processor._cast_schema(df)
        result = processor._parse_timestamps(df).select("event_ts").first()
        assert result["event_ts"] is None


# ---------------------------------------------------------------------------
# _filter_invalid
# ---------------------------------------------------------------------------

class TestFilterInvalid:

    def test_null_event_id_dropped(self, spark, processor):
        df = spark.createDataFrame([_make_event(event_id=None)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        assert processor._filter_invalid(df).count() == 0

    def test_unknown_event_type_dropped(self, spark, processor):
        df = spark.createDataFrame([_make_event(event_type="mystery_event")])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        assert processor._filter_invalid(df).count() == 0

    def test_no_user_identifier_dropped(self, spark, processor):
        df = spark.createDataFrame([_make_event(user_id=None, anonymous_id=None)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        assert processor._filter_invalid(df).count() == 0

    def test_anonymous_id_alone_is_valid(self, spark, processor):
        df = spark.createDataFrame([_make_event(user_id=None, anonymous_id="anon-xyz")])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        assert processor._filter_invalid(df).count() == 1

    @pytest.mark.parametrize("event_type", list(VALID_EVENT_TYPES))
    def test_all_valid_event_types_pass(self, spark, processor, event_type):
        df = spark.createDataFrame([_make_event(event_type=event_type)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        assert processor._filter_invalid(df).count() == 1


# ---------------------------------------------------------------------------
# _deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:

    def test_duplicate_event_id_kept_once(self, spark, processor):
        rows = [
            _make_event(event_id="evt-dup", _ingested_at="2024-01-15T10:00:00Z"),
            _make_event(event_id="evt-dup", _ingested_at="2024-01-15T10:05:00Z"),
            _make_event(event_id="evt-dup", _ingested_at="2024-01-15T10:10:00Z"),
        ]
        df = spark.createDataFrame(rows)
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._deduplicate(df)
        assert result.count() == 1

    def test_first_write_wins(self, spark, processor):
        """Earliest _ingested_at must survive deduplication."""
        rows = [
            _make_event(event_id="evt-dup", session_id="sess-A", _ingested_at="2024-01-15T10:10:00Z"),
            _make_event(event_id="evt-dup", session_id="sess-B", _ingested_at="2024-01-15T10:00:00Z"),
        ]
        df = spark.createDataFrame(rows)
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._deduplicate(df).select("session_id").first()
        assert result["session_id"] == "sess-B"  # earliest

    def test_unique_events_all_kept(self, spark, processor):
        rows = [_make_event(event_id=f"evt-{i}") for i in range(100)]
        df = spark.createDataFrame(rows)
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        assert processor._deduplicate(df).count() == 100


# ---------------------------------------------------------------------------
# _parse_urls
# ---------------------------------------------------------------------------

class TestParseUrls:

    @pytest.mark.parametrize("url, expected_domain", [
        ("https://shop.example.com/products/sneakers", "shop.example.com"),
        ("http://example.com",                          "example.com"),
        ("https://sub.domain.co.uk/path?q=1",           "sub.domain.co.uk"),
    ])
    def test_domain_extraction(self, spark, processor, url, expected_domain):
        df = spark.createDataFrame([_make_event(page_url=url)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._parse_urls(df).select("page_domain").first()
        assert result["page_domain"] == expected_domain

    @pytest.mark.parametrize("referrer, expected_channel", [
        ("",                                    "direct"),
        (None,                                  "direct"),
        ("https://www.google.com/search",        "organic_search"),
        ("https://www.facebook.com/ad/123",      "social"),
        ("https://shop.example.com/home",        "internal"),
        ("https://partner-blog.com/article",     "referral"),
    ])
    def test_referrer_channel_classification(self, spark, processor, referrer, expected_channel):
        df = spark.createDataFrame([_make_event(
            referrer_url=referrer,
            page_url="https://shop.example.com/products"
        )])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._parse_urls(df).select("referrer_channel").first()
        assert result["referrer_channel"] == expected_channel


# ---------------------------------------------------------------------------
# _anonymize_ip
# ---------------------------------------------------------------------------

class TestAnonymizeIp:

    @pytest.mark.parametrize("ip, expected", [
        ("192.168.1.42",    "192.168.1.0"),
        ("10.0.0.1",        "10.0.0.0"),
        ("255.255.255.255",  "255.255.255.0"),
    ])
    def test_ipv4_last_octet_zeroed(self, spark, processor, ip, expected):
        df = spark.createDataFrame([_make_event(ip_address=ip)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._anonymize_ip(df).select("ip_anonymized").first()
        assert result["ip_anonymized"] == expected

    def test_original_ip_column_dropped(self, spark, processor):
        df = spark.createDataFrame([_make_event(ip_address="1.2.3.4")])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._anonymize_ip(df)
        assert "ip_address" not in result.columns

    def test_null_ip_stays_null(self, spark, processor):
        df = spark.createDataFrame([_make_event(ip_address=None)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._anonymize_ip(df).select("ip_anonymized").first()
        assert result["ip_anonymized"] is None


# ---------------------------------------------------------------------------
# _parse_revenue
# ---------------------------------------------------------------------------

class TestParseRevenue:

    @pytest.mark.parametrize("raw, expected", [
        ("99.99",       99.99),
        ("€ 49,90",     49.90),    # FR locale + euro symbol
        ("USD 120.00",  120.00),
        ("£15",          15.0),
        (None,           0.0),
        ("",             None),    # empty string → null cast → 0.0 after coalesce? test checks
    ])
    def test_revenue_parsing(self, spark, processor, raw, expected):
        df = spark.createDataFrame([_make_event(revenue=raw)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._parse_revenue(df).select("revenue_eur").first()
        if expected is None:
            assert result["revenue_eur"] in (None, 0.0)
        else:
            assert abs(result["revenue_eur"] - expected) < 0.01

    def test_has_revenue_flag_set(self, spark, processor):
        df = spark.createDataFrame([_make_event(revenue="25.00")])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._parse_revenue(df).select("has_revenue").first()
        assert result["has_revenue"] is True

    def test_has_revenue_false_when_zero(self, spark, processor):
        df = spark.createDataFrame([_make_event(revenue=None)])
        df = processor._cast_schema(df)
        df = processor._parse_timestamps(df)
        df = processor._filter_invalid(df)
        result = processor._parse_revenue(df).select("has_revenue").first()
        assert result["has_revenue"] is False
