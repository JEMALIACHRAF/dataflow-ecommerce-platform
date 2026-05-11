"""
transforms/silver/pubsub_spark_consumer.py

Pub/Sub → Spark Structured Streaming → GCS Bronze/Silver
Lit les events clickstream depuis Pub/Sub et les traite
avec Spark Structured Streaming sur Databricks.

En prod : tourne en continu sur Databricks cluster
En dev  : batch mode sur les messages Pub/Sub existants
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID       = "projet-dbt-495310"
SUBSCRIPTION_ID  = "clickstream-events-sub"
BRONZE_PATH      = "C:/tmp/bronze/clickstream"
BATCH_SIZE       = 1_000
MAX_MESSAGES     = 10_000


def consume_and_write(
    max_messages: int = MAX_MESSAGES,
    batch_size:   int = BATCH_SIZE,
    output_path:  str = BRONZE_PATH,
) -> dict:
    """
    Consomme les messages Pub/Sub et écrit en Parquet Bronze.
    Simule ce que Spark Structured Streaming fait en prod sur Databricks.
    """
    subscriber    = pubsub_v1.SubscriberClient()
    subscription  = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    if not output_path.startswith("gs://"):
       os.makedirs(output_path, exist_ok=True)

    total_consumed = 0
    batch_id       = 0
    rows           = []
    output_paths   = []

    logger.info("Consuming from %s ...", subscription)

    while total_consumed < max_messages:
        pull_count = min(batch_size, max_messages - total_consumed)

        response = subscriber.pull(
            request={
                "subscription": subscription,
                "max_messages": pull_count,
            },
            timeout=30.0,
        )

        if not response.received_messages:
            logger.info("No more messages in subscription.")
            break

        ack_ids = []
        for msg in response.received_messages:
            try:
                event = json.loads(msg.message.data.decode("utf-8"))
                event["_consumed_at"] = datetime.now(timezone.utc).isoformat()
                event["_message_id"]  = msg.message.message_id
                rows.append(event)
                ack_ids.append(msg.ack_id)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON message: %s", e)

        # Acknowledge messages
        subscriber.acknowledge(
            request={"subscription": subscription, "ack_ids": ack_ids}
        )
        total_consumed += len(response.received_messages)

        # Write batch to Parquet
        if len(rows) >= batch_size:
            path = _write_parquet(rows, output_path, batch_id)
            output_paths.append(path)
            logger.info(
                "Batch %d — wrote %d events → %s",
                batch_id, len(rows), path
            )
            rows     = []
            batch_id += 1

    # Write remaining rows
    if rows:
        path = _write_parquet(rows, output_path, batch_id)
        output_paths.append(path)
        logger.info(
            "Final batch — wrote %d events → %s", len(rows), path
        )

    logger.info("✓ Consumed %d events total", total_consumed)
    return {
        "total_consumed": total_consumed,
        "files_written":  len(output_paths),
        "output_paths":   output_paths,
    }


def _write_parquet(rows: list[dict], base_path: str, batch_id: int) -> str:
    """Écrit un batch de rows en Parquet — GCS ou local."""
    import io
    date  = datetime.now().strftime("%Y-%m-%d")
    fname = f"part-{batch_id:05d}.parquet"
    table = pa.Table.from_pylist(rows)

    if base_path.startswith("gs://"):
        from google.cloud import storage as gcs
        buf    = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        bucket_name = base_path.replace("gs://", "").split("/")[0]
        prefix      = "/".join(base_path.replace("gs://", "").split("/")[1:])
        blob_path   = f"{prefix}/date={date}/{fname}"
        client      = gcs.Client()
        bucket      = client.bucket(bucket_name)
        blob        = bucket.blob(blob_path)
        blob.upload_from_file(buf, content_type="application/octet-stream")
        full_path   = f"gs://{bucket_name}/{blob_path}"
        return full_path
    else:
        path = os.path.join(base_path, f"date={date}", fname)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pq.write_table(table, path, compression="snappy")
        return path


def run_spark_silver_transform(bronze_path: str, silver_path: str, date: str) -> None:
    """
    Lance le Silver job PySpark sur les données Bronze consommées.
    En prod : tourne sur Databricks cluster avec Delta Lake.
    En dev  : tourne en local avec Spark local mode.
    """
    from pyspark.sql import SparkSession
    from transforms.silver.clickstream_processor import ClickstreamSilverProcessor

    logger.info("Starting Spark Silver transform...")
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pubsub-clickstream-silver")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    processor = ClickstreamSilverProcessor(spark, processing_date=date)
    metrics   = processor.run(
        input_path=bronze_path,
        output_path=silver_path,
    )

    logger.info("Silver transform complete: %s", metrics)
    spark.stop()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Pub/Sub consumer → Bronze Parquet → Silver Spark"
    )
    p.add_argument("--max-messages", type=int,  default=10_000)
    p.add_argument("--batch-size",   type=int,  default=1_000)
    p.add_argument("--bronze-path",  default="C:/tmp/bronze/clickstream")
    p.add_argument("--silver-path",  default="C:/tmp/silver/clickstream")
    p.add_argument("--date",         default=datetime.now().strftime("%Y-%m-%d"))
    p.add_argument("--skip-spark",   action="store_true",
                   help="Skip Spark Silver transform (consume only)")
    args = p.parse_args()

    # Step 1 : Consume Pub/Sub → Bronze Parquet
    result = consume_and_write(
        max_messages=args.max_messages,
        batch_size=args.batch_size,
        output_path=args.bronze_path,
    )

    print("\n=== RÉSUMÉ CONSOMMATION ===")
    print(f"  Events consommés : {result['total_consumed']:,}")
    print(f"  Fichiers Parquet : {result['files_written']}")
    for p_ in result["output_paths"]:
        print(f"    → {p_}")

    # Step 2 : Spark Silver transform
    if not args.skip_spark:
        run_spark_silver_transform(
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            date=args.date,
        )