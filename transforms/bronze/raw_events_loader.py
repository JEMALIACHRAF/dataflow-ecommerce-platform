"""
transforms/bronze/raw_events_loader.py

Bronze Layer — Raw Event Loader
Writes raw incoming events from Kafka topics to GCS Bronze as
append-only Parquet, with no schema enforcement or transformation.

The Bronze layer is intentionally a "data swamp" — full fidelity,
never modified after write. All cleaning happens in Silver.

Sources ingested here
---------------------
  • Kafka topic: ecom.events.clickstream  (primary — 2B events/day)
  • Kafka topic: ecom.events.orders
  • Kafka topic: ecom.events.inventory
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Max records per Parquet file (controls file size in Bronze)
CHUNK_SIZE = 100_000


class BronzeLoader:
    """
    Micro-batch Kafka → GCS Bronze loader.

    Reads from a Kafka topic, buffers records in memory, and flushes
    Parquet files to GCS when CHUNK_SIZE is reached or a time window expires.
    """

    def __init__(
        self,
        topic:          str,
        kafka_brokers:  list[str],
        gcs_bucket:     str,
        gcs_prefix:     str = "bronze",
        consumer_group: str = "bronze-loader",
        flush_interval: int = 300,          # seconds
    ) -> None:
        self.topic          = topic
        self.gcs_prefix     = gcs_prefix
        self.flush_interval = flush_interval
        self._bucket        = storage.Client().bucket(gcs_bucket)
        self._buffer:  list[dict] = []
        self._file_seq = 0

        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_brokers,
            group_id=consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,      # manual commit after GCS write
            value_deserializer=lambda b: __import__("json").loads(b.decode("utf-8")),
            consumer_timeout_ms=30_000,
        )

    def run(self) -> None:
        logger.info("Bronze loader started  topic=%s", self.topic)
        last_flush = datetime.now(timezone.utc).timestamp()

        try:
            for msg in self._consumer:
                record = self._enrich(msg)
                self._buffer.append(record)

                now = datetime.now(timezone.utc).timestamp()
                should_flush = (
                    len(self._buffer) >= CHUNK_SIZE
                    or (now - last_flush) >= self.flush_interval
                )
                if should_flush:
                    self._flush()
                    self._consumer.commit()
                    last_flush = now

        except KafkaError as exc:
            logger.error("Kafka consumer error: %s", exc)
            raise
        finally:
            if self._buffer:
                self._flush()
                self._consumer.commit()
            self._consumer.close()

    # ------------------------------------------------------------------

    def _enrich(self, msg: Any) -> dict:
        """Add Bronze metadata to each raw record."""
        record = msg.value if isinstance(msg.value, dict) else {"_raw": str(msg.value)}
        record["_ingested_at"]    = datetime.now(timezone.utc).isoformat()
        record["_kafka_partition"] = msg.partition
        record["_kafka_offset"]   = msg.offset
        record["_kafka_topic"]    = msg.topic
        record["_source_file"]    = f"kafka/{msg.topic}/partition={msg.partition}/offset={msg.offset}"
        return record

    def _flush(self) -> None:
        if not self._buffer:
            return

        date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        gcs_path  = (
            f"{self.gcs_prefix}/{self.topic}/date={date_str}/"
            f"part-{self._file_seq:05d}.parquet"
        )

        table = pa.Table.from_pylist(self._buffer)
        blob  = self._bucket.blob(gcs_path)

        with blob.open("wb") as f:
            pq.write_table(table, f, compression="snappy")

        logger.info(
            "Flushed  gs://%s/%s  (%d records)",
            self._bucket.name, gcs_path, len(self._buffer),
        )
        self._buffer  = []
        self._file_seq += 1
