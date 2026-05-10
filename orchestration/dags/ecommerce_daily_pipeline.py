"""
orchestration/dags/ecommerce_daily_pipeline.py

Main Airflow DAG — E-Commerce Daily Data Pipeline
Orchestrates the full Bronze → Silver → Gold → Snowflake → Cache flow.

Schedule  : daily at 02:00 UTC  (data available ~01:30 UTC)
SLA       : Gold layer ready by 06:00 UTC for morning dashboards
Owner     : data-engineering@dataflow.io
On-call   : PagerDuty  (P2 on SLA breach, P1 on data loss)

Dependency graph
----------------
  check_bronze_landing
        │
        ▼
  ┌─────────────────────────────────────┐
  │  bronze_to_silver_clickstream       │
  │  bronze_to_silver_orders            │  (parallel)
  │  bronze_to_silver_customers         │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │  silver_quality_check               │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │  gold_daily_sales                   │
  │  gold_funnel_metrics                │  (parallel)
  │  gold_product_performance           │
  │  gold_session_stats                 │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │  dbt_run_staging                    │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │  dbt_run_marts                      │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │  snowflake_vacuum_old_partitions     │
  │  redis_invalidate_cache             │  (parallel)
  └──────────────┬──────────────────────┘
                 │
                 ▼
  pipeline_success  (SLA gate + Slack notification)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Config  (values stored in Airflow Variables / Secret Manager in prod)
# ---------------------------------------------------------------------------

GCP_PROJECT      = Variable.get("gcp_project",      default_var="dataflow-prod")
GCP_REGION       = Variable.get("gcp_region",        default_var="europe-west1")
GCS_BRONZE       = Variable.get("gcs_bronze_bucket", default_var="gs://dataflow-bronze")
GCS_SILVER       = Variable.get("gcs_silver_bucket", default_var="gs://dataflow-silver")
GCS_GOLD         = Variable.get("gcs_gold_bucket",   default_var="gs://dataflow-gold")
DATAPROC_CLUSTER = Variable.get("dataproc_cluster",  default_var="dataflow-spark-cluster")
SNOWFLAKE_CONN   = "snowflake_prod"
SLACK_CONN       = "slack_data_alerts"
DBT_IMAGE        = "europe-docker.pkg.dev/dataflow-prod/images/dbt:1.7"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["data-engineering@dataflow.io"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "sla":              timedelta(hours=4),   # 06:00 UTC deadline
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ds(context: dict) -> str:
    """Return logical execution date as YYYY-MM-DD string."""
    return context["ds"]


def _spark_job(script_path: str, date: str, extra_args: list[str] | None = None) -> dict:
    """Build a Dataproc PySpark job definition."""
    return {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://dataflow-prod-code/{script_path}",
            "args": [
                "--date", date,
                "--input",  f"{GCS_BRONZE}/events/{date}/",
                "--output", f"{GCS_SILVER}/events/",
                *(extra_args or []),
            ],
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "gs://spark-lib/delta/delta-core_2.12-2.4.0.jar",
            ],
            "properties": {
                "spark.executor.memory":          "16g",
                "spark.executor.cores":           "4",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "50",
            },
        },
    }


def _check_bronze_volume(**context: dict) -> bool:
    """
    ShortCircuit: abort pipeline if Bronze landing is suspiciously small.
    Protects against upstream outages being silently propagated to Gold.
    """
    from google.cloud import storage

    ds = _ds(context)
    client = storage.Client()
    bucket = client.bucket("dataflow-bronze")
    blobs = list(bucket.list_blobs(prefix=f"events/{ds}/"))

    total_bytes = sum(b.size for b in blobs)
    file_count  = len(blobs)

    # Expect at least 500 files and 50 GB for a normal day
    min_files = 500
    min_bytes = 50 * 1024 ** 3

    if file_count < min_files or total_bytes < min_bytes:
        import logging
        logging.getLogger(__name__).error(
            "Bronze volume check FAILED  date=%s  files=%d (<500)  bytes=%d (<50GB)",
            ds, file_count, total_bytes,
        )
        return False   # ShortCircuit — downstream tasks skipped

    return True


def _invalidate_redis_cache(**context: dict) -> None:
    """Invalidate all cache tags touched by today's pipeline run."""
    import os
    from cache.query_cache import QueryCache

    ds = _ds(context)
    cache = QueryCache.from_url(os.environ["REDIS_URL"])
    tags = [
        f"fact_orders:{ds}",
        "gold_daily_sales",
        "gold_funnel_metrics",
        "gold_product_performance",
        "gold_session_stats",
        "dim_customers",
        "dim_products",
    ]
    total_deleted = sum(cache.invalidate_tag(tag) for tag in tags)
    context["ti"].xcom_push(key="cache_keys_deleted", value=total_deleted)


def _on_pipeline_failure(context: dict) -> None:
    """Callback — fires on any task failure; sends Slack alert."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    ds      = context["ds"]
    log_url = context["task_instance"].log_url

    SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=SLACK_CONN,
        message=(
            f":red_circle: *Pipeline failure*\n"
            f"DAG: `{dag_id}`\n"
            f"Task: `{task_id}`\n"
            f"Date: `{ds}`\n"
            f"<{log_url}|View logs>"
        ),
    ).execute(context)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ecommerce_daily_pipeline",
    description="Daily Bronze→Silver→Gold→Snowflake pipeline for e-commerce analytics",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=datetime(2023, 9, 1),
    catchup=True,
    max_active_runs=1,               # prevent overlapping runs
    tags=["ecommerce", "production", "medallion"],
    on_failure_callback=_on_pipeline_failure,
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------ #
    # Landing check                                                        #
    # ------------------------------------------------------------------ #

    wait_bronze = GCSObjectExistenceSensor(
        task_id="wait_for_bronze_landing",
        bucket="dataflow-bronze",
        object="{{ ds }}/events/_SUCCESS",
        timeout=3600,       # wait up to 1 h for upstream
        poke_interval=120,
        mode="reschedule",  # release worker slot while waiting
    )

    check_volume = ShortCircuitOperator(
        task_id="check_bronze_volume",
        python_callable=_check_bronze_volume,
    )

    # ------------------------------------------------------------------ #
    # Silver layer (parallel Spark jobs)                                   #
    # ------------------------------------------------------------------ #

    with TaskGroup("silver_layer") as silver_tg:

        silver_clickstream = DataprocSubmitJobOperator(
            task_id="silver_clickstream",
            job=_spark_job(
                "transforms/silver/clickstream_processor.py",
                date="{{ ds }}",
            ),
            region=GCP_REGION,
            project_id=GCP_PROJECT,
        )

        silver_orders = DataprocSubmitJobOperator(
            task_id="silver_orders",
            job=_spark_job(
                "transforms/silver/orders_processor.py",
                date="{{ ds }}",
                extra_args=["--input", f"{GCS_BRONZE}/orders/{{{{ ds }}}}/"],
            ),
            region=GCP_REGION,
            project_id=GCP_PROJECT,
        )

        silver_customers = DataprocSubmitJobOperator(
            task_id="silver_customers",
            job=_spark_job(
                "transforms/silver/customers_processor.py",
                date="{{ ds }}",
                extra_args=["--input", f"{GCS_BRONZE}/customers/{{{{ ds }}}}/"],
            ),
            region=GCP_REGION,
            project_id=GCP_PROJECT,
        )

    # ------------------------------------------------------------------ #
    # Data quality gate                                                    #
    # ------------------------------------------------------------------ #

    quality_check = SnowflakeOperator(
        task_id="silver_quality_check",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            -- Fail the task (zero rows returned = assertion passed in Airflow)
            -- Any row returned = quality violation = task fails
            SELECT
                'clickstream_null_event_id'     AS check_name,
                COUNT(*)                         AS failing_rows
            FROM SILVER.CLICKSTREAM
            WHERE event_date = '{{ ds }}'
              AND event_id IS NULL

            UNION ALL

            SELECT
                'orders_negative_revenue',
                COUNT(*)
            FROM SILVER.ORDERS
            WHERE order_date = '{{ ds }}'
              AND revenue_eur < 0

            UNION ALL

            SELECT
                'daily_order_volume_too_low',
                CASE WHEN COUNT(*) < 100 THEN 1 ELSE 0 END
            FROM SILVER.ORDERS
            WHERE order_date = '{{ ds }}'

            HAVING SUM(failing_rows) > 0;
        """,
    )

    # ------------------------------------------------------------------ #
    # Gold layer (parallel aggregation jobs)                               #
    # ------------------------------------------------------------------ #

    with TaskGroup("gold_layer") as gold_tg:

        for agg in (
            "daily_sales",
            "funnel_metrics",
            "product_performance",
            "session_stats",
        ):
            DataprocSubmitJobOperator(
                task_id=f"gold_{agg}",
                job={
                    "reference": {"project_id": GCP_PROJECT},
                    "placement": {"cluster_name": DATAPROC_CLUSTER},
                    "pyspark_job": {
                        "main_python_file_uri":
                            "gs://dataflow-prod-code/transforms/gold/daily_sales_aggregator.py",
                        "args": ["--date", "{{ ds }}", "--agg", agg],
                    },
                },
                region=GCP_REGION,
                project_id=GCP_PROJECT,
            )

    # ------------------------------------------------------------------ #
    # dbt — Snowflake star schema                                          #
    # ------------------------------------------------------------------ #

    dbt_staging = SnowflakeOperator(
        task_id="dbt_run_staging",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="CALL DBT.RUN_MODELS('staging', '{{ ds }}');",
    )

    dbt_marts = SnowflakeOperator(
        task_id="dbt_run_marts",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="CALL DBT.RUN_MODELS('marts', '{{ ds }}');",
    )

    dbt_test = SnowflakeOperator(
        task_id="dbt_test",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="CALL DBT.RUN_TESTS('marts', '{{ ds }}');",
    )

    # ------------------------------------------------------------------ #
    # Post-processing (parallel)                                           #
    # ------------------------------------------------------------------ #

    with TaskGroup("post_processing") as post_tg:

        snowflake_vacuum = SnowflakeOperator(
            task_id="snowflake_vacuum",
            snowflake_conn_id=SNOWFLAKE_CONN,
            sql="""
                -- Drop Silver staging tables older than 7 days to control storage costs
                CALL UTILS.DROP_OLD_STAGING_TABLES(7);
                -- Update table statistics for query optimiser
                ALTER TABLE GOLD.FACT_ORDERS CLUSTER BY (order_date, country_code);
            """,
        )

        redis_invalidate = PythonOperator(
            task_id="redis_invalidate_cache",
            python_callable=_invalidate_redis_cache,
        )

    # ------------------------------------------------------------------ #
    # Success gate + Slack notification                                    #
    # ------------------------------------------------------------------ #

    pipeline_success = SlackWebhookOperator(
        task_id="pipeline_success_notification",
        slack_webhook_conn_id=SLACK_CONN,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        message=(
            ":large_green_circle: *Daily pipeline complete*\n"
            "Date: `{{ ds }}`\n"
            "Gold layer ready for dashboards."
        ),
    )

    # ------------------------------------------------------------------ #
    # Dependency wiring                                                    #
    # ------------------------------------------------------------------ #

    wait_bronze >> check_volume >> silver_tg >> quality_check
    quality_check >> gold_tg >> dbt_staging >> dbt_marts >> dbt_test
    dbt_test >> post_tg >> pipeline_success
