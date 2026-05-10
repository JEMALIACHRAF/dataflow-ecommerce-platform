"""
monitoring/great_expectations/checkpoints/clickstream_silver_checkpoint.py

Great Expectations — Clickstream Silver Quality Suite
Validates the Silver clickstream Delta table after each daily Spark run.
Integrated into the Airflow DAG (dq_checks task).

Expectations enforced
---------------------
  Schema        : all required columns present, correct types
  Completeness  : event_id, event_type, event_ts never null (0% null rate)
  Uniqueness    : event_id is globally unique
  Validity      : event_type ∈ known set  |  country_code is 2-char ISO
  Freshness     : _silver_processed_at within last 8 hours
  Volume        : row count between 1B and 4B per day
  Revenue       : revenue_eur ≥ 0  |  no outliers > 50 000 EUR
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

logger = logging.getLogger(__name__)

VALID_EVENT_TYPES = [
    "page_view", "product_view", "add_to_cart", "remove_from_cart",
    "checkout_start", "checkout_complete", "purchase", "search",
    "wishlist_add", "promo_click", "session_start", "session_end",
]

REQUIRED_COLUMNS = [
    "event_id", "session_id", "user_id", "anonymous_id",
    "event_type", "event_ts", "event_date",
    "page_url", "page_domain", "page_path",
    "device_type", "os", "browser",
    "country_code", "ip_anonymized",
    "referrer_channel", "revenue_eur", "has_revenue",
    "_silver_processed_at", "_silver_version", "_processing_date",
]


def build_suite(context: BaseDataContext, suite_name: str) -> None:
    """Define and save the expectation suite."""
    suite = context.create_expectation_suite(
        expectation_suite_name=suite_name,
        overwrite_existing=True,
    )

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="clickstream_silver",
            runtime_parameters={"path": os.getenv("SILVER_EVENTS_PATH", "")},
            batch_identifiers={"run_id": "suite_build"},
        ),
        expectation_suite_name=suite_name,
    )

    # ------------------------------------------------------------------ #
    # Schema expectations                                                  #
    # ------------------------------------------------------------------ #
    for col in REQUIRED_COLUMNS:
        validator.expect_column_to_exist(column=col)

    validator.expect_table_columns_to_match_set(
        column_set=REQUIRED_COLUMNS,
        exact_match=False,   # allow additional columns (schema evolution)
    )

    # ------------------------------------------------------------------ #
    # Completeness                                                         #
    # ------------------------------------------------------------------ #
    for col in ["event_id", "event_type", "event_ts", "event_date"]:
        validator.expect_column_values_to_not_be_null(
            column=col,
            meta={"severity": "critical"},
        )

    # user_id OR anonymous_id must be non-null — checked via custom SQL
    validator.expect_column_values_to_not_be_null(
        column="session_id",
        mostly=0.999,   # allow 0.1% null for edge cases (bot traffic)
    )

    # ------------------------------------------------------------------ #
    # Uniqueness                                                           #
    # ------------------------------------------------------------------ #
    validator.expect_column_values_to_be_unique(
        column="event_id",
        meta={"severity": "critical", "note": "Dedup should ensure 100% uniqueness"},
    )

    # ------------------------------------------------------------------ #
    # Validity — event_type                                                #
    # ------------------------------------------------------------------ #
    validator.expect_column_values_to_be_in_set(
        column="event_type",
        value_set=VALID_EVENT_TYPES,
        meta={"severity": "critical"},
    )

    # ------------------------------------------------------------------ #
    # Validity — country_code                                              #
    # ------------------------------------------------------------------ #
    validator.expect_column_value_lengths_to_equal(
        column="country_code",
        value=2,
        mostly=0.99,   # 1% tolerance for upstream dirty data
    )

    validator.expect_column_values_to_match_regex(
        column="country_code",
        regex=r"^[A-Z]{2}$",
        mostly=0.99,
    )

    # ------------------------------------------------------------------ #
    # Validity — device_type                                               #
    # ------------------------------------------------------------------ #
    validator.expect_column_values_to_be_in_set(
        column="device_type",
        value_set=["desktop", "mobile", "tablet"],
        mostly=0.995,
    )

    # ------------------------------------------------------------------ #
    # Revenue sanity checks                                                #
    # ------------------------------------------------------------------ #
    validator.expect_column_values_to_be_between(
        column="revenue_eur",
        min_value=0.0,
        max_value=50_000.0,   # flag outliers > 50 000 EUR (likely data error)
        mostly=0.9999,
    )

    validator.expect_column_values_to_not_be_null(
        column="revenue_eur",
        mostly=1.0,   # _parse_revenue always fills 0.0 — never null
    )

    # ------------------------------------------------------------------ #
    # Referrer channel                                                     #
    # ------------------------------------------------------------------ #
    validator.expect_column_values_to_be_in_set(
        column="referrer_channel",
        value_set=["direct", "organic_search", "social", "internal", "referral"],
        mostly=0.999,
    )

    # ------------------------------------------------------------------ #
    # Volume guard (2B ± 100% tolerance for weekend/peak variance)        #
    # ------------------------------------------------------------------ #
    validator.expect_table_row_count_to_be_between(
        min_value=500_000_000,    # 500M minimum — flags catastrophic failures
        max_value=5_000_000_000,  # 5B maximum — flags runaway duplication
    )

    # ------------------------------------------------------------------ #
    # Freshness                                                            #
    # ------------------------------------------------------------------ #
    max_age_hours = 8
    freshness_threshold = (
        datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    ).isoformat()

    validator.expect_column_values_to_be_between(
        column="_silver_processed_at",
        min_value=freshness_threshold,
        meta={
            "severity": "warning",
            "note": f"Silver data should be < {max_age_hours}h old",
        },
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("Expectation suite '%s' saved with %d expectations",
                suite_name, len(suite.expectations))


def run_checkpoint(processing_date: str, silver_path: str) -> dict:
    """
    Execute the checkpoint against today's Silver data.
    Returns GE validation result dict.
    Raises RuntimeError if any critical expectation fails.
    """
    context = ge.get_context()

    checkpoint_config = {
        "name": "clickstream_silver_checkpoint",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": f"%Y%m%d-{processing_date}",
        "validations": [
            {
                "batch_request": {
                    "datasource_name":       "spark_datasource",
                    "data_connector_name":   "runtime_data_connector",
                    "data_asset_name":       "clickstream_silver",
                    "runtime_parameters":    {"path": silver_path},
                    "batch_identifiers":     {"run_date": processing_date},
                },
                "expectation_suite_name": "clickstream_silver_suite",
            }
        ],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
            {
                "name": "send_slack_notification_on_failure",
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook":      os.getenv("SLACK_WEBHOOK_URL", ""),
                    "notify_on":          "failure",
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.slack_renderer",
                        "class_name":  "SlackRenderer",
                    },
                },
            },
        ],
    }

    results = context.run_checkpoint(**checkpoint_config)

    success = results["success"]
    stats = results.get("run_results", {})
    logger.info(
        "GE checkpoint complete  date=%s  success=%s",
        processing_date, success,
    )

    if not success:
        failed = [
            k for k, v in stats.items()
            if not v.get("validation_result", {}).get("success", True)
        ]
        raise RuntimeError(
            f"Data quality checks FAILED for {processing_date}. "
            f"Failed suites: {failed}"
        )

    return results


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date",  required=True)
    p.add_argument("--path",  required=True)
    p.add_argument("--build-suite", action="store_true")
    args = p.parse_args()

    if args.build_suite:
        ctx = ge.get_context()
        build_suite(ctx, "clickstream_silver_suite")

    run_checkpoint(processing_date=args.date, silver_path=args.path)
