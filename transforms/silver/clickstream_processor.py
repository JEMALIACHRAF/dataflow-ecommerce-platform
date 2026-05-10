"""
transforms/silver/clickstream_processor.py

Silver Layer — Clickstream Event Processor
Processes raw clickstream events from Bronze (GCS Parquet) into cleaned,
typed, deduplicated Silver Delta data ready for Gold aggregations.

Scale  : ~2 billion events/day  (~24 000 events/sec peak)
Runtime: Databricks Runtime 14.x  (Spark 3.5)
Output : GCS Delta, partitioned by event_date / event_type
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType(
    [
        StructField("event_id",        StringType(), nullable=True),
        StructField("session_id",      StringType(), nullable=True),
        StructField("user_id",         StringType(), nullable=True),
        StructField("anonymous_id",    StringType(), nullable=True),
        StructField("event_type",      StringType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),   # raw string
        StructField("page_url",        StringType(), nullable=True),
        StructField("referrer_url",    StringType(), nullable=True),
        StructField("device_type",     StringType(), nullable=True),
        StructField("os",              StringType(), nullable=True),
        StructField("browser",         StringType(), nullable=True),
        StructField("country_code",    StringType(), nullable=True),
        StructField("ip_address",      StringType(), nullable=True),
        StructField("properties",      StringType(), nullable=True),   # JSON blob
        StructField("revenue",         StringType(), nullable=True),   # raw numeric
        StructField("_ingested_at",    StringType(), nullable=True),
        StructField("_source_file",    StringType(), nullable=True),
    ]
)

VALID_EVENT_TYPES = frozenset(
    {
        "page_view", "product_view", "add_to_cart", "remove_from_cart",
        "checkout_start", "checkout_complete", "purchase", "search",
        "wishlist_add", "promo_click", "session_start", "session_end",
    }
)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session(app_name: str = "clickstream-silver") -> SparkSession:
    """
    Build a SparkSession configured for GCS + Delta Lake.
    On Databricks the session already exists — .getOrCreate() is a safe no-op.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "800")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class ClickstreamSilverProcessor:
    """
    Bronze → Silver transformation pipeline for clickstream events.

    Processing steps
    ----------------
    1. Schema enforcement
    2. Timestamp parsing  (ISO 8601, Unix-ms, Unix-s)
    3. Data-quality filtering
    4. Idempotent deduplication on event_id
    5. URL parsing + referrer channel classification
    6. GDPR IP anonymisation (last IPv4 octet zeroed)
    7. Revenue normalisation  (multi-currency, FR locale decimals)
    8. Partitioned Delta write with replaceWhere for safe reruns
    """

    def __init__(self, spark: SparkSession, processing_date: str) -> None:
        self.spark = spark
        self.processing_date = processing_date
        self._metrics: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self, input_path: str, output_path: str) -> dict[str, int]:
        logger.info(
            "Silver job start  date=%s  input=%s  output=%s",
            self.processing_date, input_path, output_path,
        )

        df = self._read_bronze(input_path)
        self._metrics["bronze_count"] = df.count()
        logger.info("Bronze records loaded : %d", self._metrics["bronze_count"])

        df_silver = (
            df
            .transform(self._cast_schema)
            .transform(self._parse_timestamps)
            .transform(self._filter_invalid)
            .transform(self._deduplicate)
            .transform(self._parse_urls)
            .transform(self._anonymize_ip)
            .transform(self._parse_revenue)
            .transform(self._add_metadata)
        )

        self._metrics["silver_count"] = df_silver.count()
        self._metrics["dropped_count"] = (
            self._metrics["bronze_count"] - self._metrics["silver_count"]
        )
        drop_pct = 100 * self._metrics["dropped_count"] / max(self._metrics["bronze_count"], 1)
        logger.info(
            "Silver records: %d  (dropped %d / %.1f%%)",
            self._metrics["silver_count"],
            self._metrics["dropped_count"],
            drop_pct,
        )

        self._write_silver(df_silver, output_path)
        logger.info("Silver job complete.")
        return self._metrics

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def _read_bronze(self, path: str) -> DataFrame:
        return (
            self.spark.read
            .schema(BRONZE_SCHEMA)
            .option("mergeSchema", "true")
            .parquet(path)
        )

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------

    def _cast_schema(self, df: DataFrame) -> DataFrame:
        """Normalise casing; invalid casts surface as null (permissive)."""
        return df.select(
            F.col("event_id"),
            F.col("session_id"),
            F.col("user_id"),
            F.col("anonymous_id"),
            F.lower(F.trim(F.col("event_type"))).alias("event_type"),
            F.col("event_timestamp"),
            F.col("page_url"),
            F.col("referrer_url"),
            F.lower(F.trim(F.col("device_type"))).alias("device_type"),
            F.lower(F.trim(F.col("os"))).alias("os"),
            F.lower(F.trim(F.col("browser"))).alias("browser"),
            F.upper(F.trim(F.col("country_code"))).alias("country_code"),
            F.col("ip_address"),
            F.col("properties"),
            F.col("revenue"),
            F.col("_ingested_at"),
            F.col("_source_file"),
        )

    def _parse_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Handle heterogeneous timestamp formats from 15+ upstream sources.

        Priority order:
          1. ISO 8601 with milliseconds + TZ
          2. ISO 8601 with TZ
          3. ISO 8601 without TZ
          4. Unix milliseconds (13-digit string)
          5. Unix seconds     (10-digit string)
          → null  →  row dropped downstream
        """
        ts = F.col("event_timestamp")
        return (
            df
            .withColumn(
                "event_ts",
                F.coalesce(
                    F.to_timestamp(ts, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                    F.to_timestamp(ts, "yyyy-MM-dd'T'HH:mm:ssZ"),
                    F.to_timestamp(ts, "yyyy-MM-dd'T'HH:mm:ss"),
                    F.when(
                        ts.rlike(r"^\d{13}$"),
                        F.to_timestamp(
                            (ts.cast(LongType()) / 1000).cast(LongType())
                        ),
                    ),
                    F.when(
                        ts.rlike(r"^\d{10}$"),
                        F.to_timestamp(ts.cast(LongType())),
                    ),
                ),
            )
            .withColumn("event_date", F.to_date(F.col("event_ts")))
        )

    def _filter_invalid(self, df: DataFrame) -> DataFrame:
        """
        Drop records that violate data contracts.
        All four conditions are logged separately in production via
        a companion _audit_filter() call (omitted here for brevity).
        """
        return df.filter(
            F.col("event_id").isNotNull()
            & (F.length(F.col("event_id")) > 0)
            & F.col("event_ts").isNotNull()
            & F.col("event_type").isin(list(VALID_EVENT_TYPES))
            # At least one user identifier must be present
            & (F.col("user_id").isNotNull() | F.col("anonymous_id").isNotNull())
            # Sanity-check: event must fall in a credible time window
            & (F.col("event_ts") >= F.lit("2020-01-01").cast(TimestampType()))
            & (F.col("event_ts") <= F.current_timestamp())
        )

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Idempotent dedup by event_id.
        First-write-wins (earliest _ingested_at) ensures deterministic reruns.
        """
        w = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").asc_nulls_last())
        return (
            df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    def _parse_urls(self, df: DataFrame) -> DataFrame:
        """Extract domain, path and classify referrer channel."""
        domain_re = r"^(?:https?://)?([^/?#]+)"
        path_re   = r"^(?:https?://)?[^/?#]+(/[^?#]*)"

        return (
            df
            .withColumn("page_domain",
                        F.regexp_extract(F.col("page_url"), domain_re, 1))
            .withColumn("page_path",
                        F.regexp_extract(F.col("page_url"), path_re, 1))
            .withColumn("referrer_domain",
                        F.regexp_extract(
                            F.coalesce(F.col("referrer_url"), F.lit("")),
                            domain_re, 1,
                        ))
            .withColumn(
                "referrer_channel",
                F.when(F.col("referrer_domain") == "", "direct")
                 .when(F.col("referrer_domain").rlike(
                     r"google|bing|yahoo|duckduckgo"), "organic_search")
                 .when(F.col("referrer_domain").rlike(
                     r"facebook|instagram|twitter|tiktok|linkedin"), "social")
                 .when(F.col("referrer_domain") == F.col("page_domain"), "internal")
                 .otherwise("referral"),
            )
        )

    def _anonymize_ip(self, df: DataFrame) -> DataFrame:
        """
        GDPR Art. 25 — privacy by design.
        IPv4 : zero last octet  (e.g. 1.2.3.4 → 1.2.3.0)
        IPv6 : zero last 80 bits
        Original ip_address column is dropped.
        """
        return (
            df
            .withColumn(
                "ip_anonymized",
                F.when(
                    F.col("ip_address").rlike(r"^\d{1,3}(\.\d{1,3}){3}$"),
                    F.regexp_replace(F.col("ip_address"), r"\.\d+$", ".0"),
                )
                .when(
                    F.col("ip_address").isNotNull(),
                    F.regexp_replace(
                        F.col("ip_address"),
                        r"(:[0-9a-fA-F]{0,4}){5}$",
                        ":0:0:0:0:0",
                    ),
                )
                .otherwise(F.lit(None).cast(StringType())),
            )
            .drop("ip_address")
        )

    def _parse_revenue(self, df: DataFrame) -> DataFrame:
        """
        Normalise revenue field to float EUR.
        Handles: currency symbols (€ $ £), ISO codes (EUR USD GBP),
        French decimal comma (3,99 → 3.99).
        """
        cleaned = F.regexp_replace(
            F.regexp_replace(
                F.col("revenue"),
                r"[€$£]|EUR|USD|GBP|\s", "",
            ),
            r",", ".",
        )
        return (
            df
            .withColumn(
                "revenue_eur",
                F.when(
                    F.col("revenue").isNotNull(),
                    cleaned.cast(DoubleType()),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("has_revenue", F.col("revenue_eur") > 0)
            .drop("revenue")
        )

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("_silver_processed_at", F.current_timestamp())
            .withColumn("_silver_version", F.lit("2.4.1"))
            .withColumn("_processing_date", F.lit(self.processing_date))
        )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def _write_silver(self, df: DataFrame, output_path: str) -> None:
        """
        Delta write with replaceWhere — safe for daily reruns without
        dropping unrelated partitions.
        Repartition to ~200 files per day (~10 MB each at 2B events).
        """
        (
            df
            .repartition(200, F.col("event_date"), F.col("event_type"))
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .option("replaceWhere", f"_processing_date = '{self.processing_date}'")
            .partitionBy("event_date", "event_type")
            .save(output_path)
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clickstream Bronze → Silver")
    p.add_argument("--input",  required=True,
                   help="GCS Bronze path, e.g. gs://dataflow-bronze/events/2024-01-15/")
    p.add_argument("--output", required=True,
                   help="GCS Silver path, e.g. gs://dataflow-silver/events/")
    p.add_argument("--date",
                   default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                   help="Processing date YYYY-MM-DD (default: today UTC)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    spark = create_spark_session()
    processor = ClickstreamSilverProcessor(spark, processing_date=args.date)
    metrics = processor.run(input_path=args.input, output_path=args.output)

    # Emit metrics to Datadog via DogStatsD sidecar on GKE pod
    try:
        from datadog import statsd  # type: ignore
        tags = [f"date:{args.date}", "layer:silver", "job:clickstream"]
        statsd.gauge("pipeline.bronze_count",  metrics["bronze_count"],  tags=tags)
        statsd.gauge("pipeline.silver_count",  metrics["silver_count"],  tags=tags)
        statsd.gauge("pipeline.dropped_count", metrics["dropped_count"], tags=tags)
    except ImportError:
        logger.warning("datadog not installed — metrics not emitted to Datadog")

    spark.stop()


if __name__ == "__main__":
    main()
