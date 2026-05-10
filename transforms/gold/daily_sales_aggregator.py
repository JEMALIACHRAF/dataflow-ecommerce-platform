"""
transforms/gold/daily_sales_aggregator.py

Gold Layer — Daily Sales & Behavioral Aggregations
Reads Silver clickstream + Silver orders → produces four Gold tables
written to both BigQuery (analytics serving) and Snowflake staging.

Tables produced
---------------
  gold_daily_sales         Revenue, orders, AOV by country/channel/device
  gold_funnel_metrics      Conversion funnel view→cart→checkout→purchase
  gold_product_performance Top products: views, ATC rate, revenue
  gold_session_stats       Duration, depth, bounce rate
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


@dataclass
class GoldConfig:
    processing_date: str
    silver_events_path: str
    silver_orders_path: str
    gold_output_path: str
    bigquery_project: str
    bigquery_dataset: str
    snowflake_options: dict = field(default_factory=dict)


class DailySalesAggregator:
    """Gold-layer aggregation job."""

    FUNNEL_STEPS = ("product_view", "add_to_cart", "checkout_start", "purchase")

    def __init__(self, spark: SparkSession, config: GoldConfig) -> None:
        self.spark = spark
        self.cfg = config

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run(self) -> dict[str, int]:
        df_events = self._read_silver_events()  # cached — used by 3 aggs
        df_orders = self._read_silver_orders()

        tables: dict[str, DataFrame] = {
            "gold_daily_sales":         self._daily_sales(df_events, df_orders),
            "gold_funnel_metrics":      self._funnel_metrics(df_events),
            "gold_product_performance": self._product_performance(df_events, df_orders),
            "gold_session_stats":       self._session_stats(df_events),
        }

        row_counts: dict[str, int] = {}
        for name, df in tables.items():
            count = df.count()
            row_counts[name] = count
            self._write_bigquery(df, name)
            self._write_snowflake_staging(df, name)
            logger.info("Written %-35s — %d rows", name, count)

        df_events.unpersist()
        return row_counts

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def _read_silver_events(self) -> DataFrame:
        return (
            self.spark.read.format("delta")
            .load(self.cfg.silver_events_path)
            .filter(F.col("_processing_date") == self.cfg.processing_date)
            .cache()
        )

    def _read_silver_orders(self) -> DataFrame:
        return (
            self.spark.read.format("delta")
            .load(self.cfg.silver_orders_path)
            .filter(F.col("order_date") == self.cfg.processing_date)
        )

    # ------------------------------------------------------------------
    # Aggregations
    # ------------------------------------------------------------------

    def _daily_sales(self, df_events: DataFrame, df_orders: DataFrame) -> DataFrame:
        """Revenue KPIs with last-touch session attribution."""
        session_attr = (
            df_events
            .filter(F.col("event_type") == "session_start")
            .select("session_id", "country_code", "device_type",
                    "referrer_channel", "user_id")
        )

        return (
            df_orders
            .join(session_attr, on="session_id", how="left")
            .groupBy(
                F.lit(self.cfg.processing_date).alias("report_date"),
                "country_code", "device_type", "referrer_channel",
            )
            .agg(
                F.count("order_id").alias("total_orders"),
                F.countDistinct("user_id").alias("unique_buyers"),
                F.sum("revenue_eur").alias("total_revenue_eur"),
                F.avg("revenue_eur").alias("avg_order_value_eur"),
                F.percentile_approx("revenue_eur", 0.5).alias("median_order_value_eur"),
                F.sum(F.when(F.col("is_first_order"), 1).otherwise(0))
                 .alias("new_customer_orders"),
                F.sum(F.when(~F.col("is_first_order"), 1).otherwise(0))
                 .alias("returning_customer_orders"),
            )
            .withColumn(
                "new_customer_rate",
                F.round(F.col("new_customer_orders") / F.col("total_orders"), 4),
            )
            .withColumn("_gold_processed_at", F.current_timestamp())
        )

    def _funnel_metrics(self, df_events: DataFrame) -> DataFrame:
        """Session-level conversion funnel."""
        session_funnel = (
            df_events
            .filter(F.col("event_type").isin(list(self.FUNNEL_STEPS)))
            .groupBy("session_id", "country_code", "device_type")
            .agg(
                *[
                    F.max(F.when(F.col("event_type") == step, 1).otherwise(0))
                     .alias(f"reached_{step}")
                    for step in self.FUNNEL_STEPS
                ]
            )
        )

        return (
            session_funnel
            .groupBy(
                F.lit(self.cfg.processing_date).alias("report_date"),
                "country_code", "device_type",
            )
            .agg(
                F.count("session_id").alias("total_sessions"),
                F.sum("reached_product_view").alias("sessions_product_view"),
                F.sum("reached_add_to_cart").alias("sessions_add_to_cart"),
                F.sum("reached_checkout_start").alias("sessions_checkout_start"),
                F.sum("reached_purchase").alias("sessions_purchase"),
            )
            .withColumn(
                "view_to_cart_rate",
                F.round(F.col("sessions_add_to_cart") / F.col("sessions_product_view"), 4),
            )
            .withColumn(
                "cart_to_checkout_rate",
                F.round(F.col("sessions_checkout_start") / F.col("sessions_add_to_cart"), 4),
            )
            .withColumn(
                "checkout_to_purchase_rate",
                F.round(F.col("sessions_purchase") / F.col("sessions_checkout_start"), 4),
            )
            .withColumn(
                "overall_conversion_rate",
                F.round(F.col("sessions_purchase") / F.col("total_sessions"), 4),
            )
            .withColumn("_gold_processed_at", F.current_timestamp())
        )

    def _product_performance(
        self, df_events: DataFrame, df_orders: DataFrame
    ) -> DataFrame:
        """Product-level engagement and revenue metrics."""
        product_events = (
            df_events
            .filter(F.col("event_type").isin(["product_view", "add_to_cart"]))
            .withColumn("product_id",
                        F.get_json_object(F.col("properties"), "$.product_id"))
            .filter(F.col("product_id").isNotNull())
            .groupBy("product_id")
            .agg(
                F.sum(F.when(F.col("event_type") == "product_view",   1).otherwise(0))
                 .alias("views"),
                F.sum(F.when(F.col("event_type") == "add_to_cart",    1).otherwise(0))
                 .alias("add_to_cart_events"),
                F.countDistinct(
                    F.when(F.col("event_type") == "product_view", F.col("user_id"))
                ).alias("unique_viewers"),
            )
        )

        # Orders have a line_items array column: [{product_id, qty, unit_price}, ...]
        product_revenue = (
            df_orders
            .select(F.explode("line_items").alias("item"))
            .select(
                F.col("item.product_id").alias("product_id"),
                F.col("item.quantity").alias("quantity"),
                F.col("item.unit_price_eur").alias("unit_price_eur"),
                (F.col("item.quantity") * F.col("item.unit_price_eur"))
                .alias("item_revenue_eur"),
            )
            .groupBy("product_id")
            .agg(
                F.sum("quantity").alias("units_sold"),
                F.sum("item_revenue_eur").alias("product_revenue_eur"),
            )
        )

        return (
            product_events
            .join(product_revenue, on="product_id", how="left")
            .withColumn("add_to_cart_rate",
                        F.round(F.col("add_to_cart_events") / F.col("views"), 4))
            .withColumn("purchase_rate",
                        F.round(F.col("units_sold") / F.col("unique_viewers"), 4))
            .withColumn("report_date", F.lit(self.cfg.processing_date))
            .withColumn("_gold_processed_at", F.current_timestamp())
        )

    def _session_stats(self, df_events: DataFrame) -> DataFrame:
        """Session quality: duration, depth, bounce rate."""
        sessions = (
            df_events
            .groupBy("session_id", "country_code", "device_type", "referrer_channel")
            .agg(
                F.min("event_ts").alias("session_start"),
                F.max("event_ts").alias("session_end"),
                F.count("*").alias("event_count"),
                F.countDistinct("page_path").alias("unique_pages"),
                F.max(F.when(F.col("has_revenue"), 1).otherwise(0))
                 .alias("has_purchase"),
            )
            .withColumn(
                "duration_s",
                F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
            )
            .withColumn(
                "is_bounce",
                (F.col("unique_pages") == 1) & (F.col("duration_s") < 30),
            )
        )

        return (
            sessions
            .groupBy(
                F.lit(self.cfg.processing_date).alias("report_date"),
                "country_code", "device_type", "referrer_channel",
            )
            .agg(
                F.count("session_id").alias("total_sessions"),
                F.avg("duration_s").alias("avg_session_duration_s"),
                F.avg("unique_pages").alias("avg_pages_per_session"),
                F.avg("event_count").alias("avg_events_per_session"),
                F.avg(F.col("is_bounce").cast("int")).alias("bounce_rate"),
                F.sum(F.col("has_purchase").cast("int"))
                 .alias("sessions_with_purchase"),
            )
            .withColumn("_gold_processed_at", F.current_timestamp())
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def _write_bigquery(self, df: DataFrame, table_name: str) -> None:
        full_table = (
            f"{self.cfg.bigquery_project}.{self.cfg.bigquery_dataset}.{table_name}"
        )
        (
            df.write
            .format("bigquery")
            .option("table", full_table)
            .option("writeMethod", "direct")
            .option("createDisposition", "CREATE_IF_NEEDED")
            .option("writeDisposition", "WRITE_TRUNCATE")
            .option("partitionField", "report_date")
            .option("partitionType", "DAY")
            .mode("overwrite")
            .save()
        )
        logger.info("BigQuery  → %s", full_table)

    def _write_snowflake_staging(self, df: DataFrame, table_name: str) -> None:
        staging = f"STG_{table_name.upper()}"
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**self.cfg.snowflake_options)
            .option("dbtable", staging)
            .mode("overwrite")
            .save()
        )
        logger.info("Snowflake → %s", staging)
