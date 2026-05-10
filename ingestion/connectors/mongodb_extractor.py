"""
ingestion/connectors/mongodb_extractor.py

MongoDB → Snowflake User Profile Extractor
Extracts semi-structured user profiles from MongoDB and stages them
in Snowflake for hourly synchronisation (triggered by Airbyte scheduler).

Collections extracted
---------------------
  users.profiles     — base profile (email, segment, preferences)
  users.events_meta  — aggregated behavioural signals (last_seen, ltv_estimate)
  users.consents     — GDPR consent flags per channel

Storage modes (auto-detected from environment)
----------------------------------------------
  PROD  : MongoDB Atlas → GCS Bronze (Parquet) → Snowflake
  LOCAL : MongoDB Atlas → local Parquet (C:/tmp/bronze/) → Snowflake
          (fallback automatique si GCS billing non activé)

Forcer un mode dans .env :
  STORAGE_MODE=gcs    → force GCS (nécessite billing activé)
  STORAGE_MODE=local  → force local
  STORAGE_MODE=auto   → détecte automatiquement (défaut)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

BATCH_SIZE = 5_000


# ---------------------------------------------------------------------------
# Storage backend auto-detection
# ---------------------------------------------------------------------------

def _detect_storage_mode() -> str:
    """
    Auto-détecte le mode de stockage :
    - STORAGE_MODE=gcs   → GCS prod
    - STORAGE_MODE=local → local fallback
    - STORAGE_MODE=auto  → essaie GCS, fallback local si billing absent
    """
    forced = os.getenv("STORAGE_MODE", "auto").lower()
    if forced in ("gcs", "local"):
        logger.info("Mode storage forcé : %s", forced)
        return forced

    # Auto : teste si GCS est disponible
    try:
        from google.cloud import storage as gcs
        client = gcs.Client()
        client.list_buckets(max_results=1)
        logger.info("GCS disponible → mode PROD activé")
        return "gcs"
    except Exception as e:
        logger.warning(
            "GCS non disponible (%s) → fallback mode LOCAL\n"
            "  Pour activer GCS : activez le billing sur console.cloud.google.com",
            type(e).__name__,
        )
        return "local"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ExtractionResult:
    collection:     str
    docs_extracted: int
    docs_upserted:  int
    duration_s:     float
    watermark:      datetime
    storage_mode:   str
    output_paths:   list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Storage writers
# ---------------------------------------------------------------------------

class GCSWriter:
    """Écrit les données en Parquet sur GCS (mode prod)."""

    def __init__(self, bucket: str, prefix: str = "mongodb") -> None:
        from google.cloud import storage as gcs
        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket.replace("gs://", ""))
        self._prefix = prefix

    def write_batch(
        self, rows: list[dict], collection: str, batch_id: int, date: str
    ) -> str:
        import io
        table  = pa.Table.from_pylist(rows)
        path   = f"{self._prefix}/{collection}/date={date}/part-{batch_id:05d}.parquet"
        buf    = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        blob = self._bucket.blob(path)
        blob.upload_from_file(buf, content_type="application/octet-stream")
        full = f"gs://{self._bucket.name}/{path}"
        logger.info("GCS ← %d rows → %s", len(rows), full)
        return full


class LocalWriter:
    """Écrit les données en Parquet localement (fallback)."""

    def __init__(self, base_path: str = "C:/tmp/bronze") -> None:
        self._base = Path(base_path)

    def write_batch(
        self, rows: list[dict], collection: str, batch_id: int, date: str
    ) -> str:
        out_dir = self._base / collection / f"date={date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path  = out_dir / f"part-{batch_id:05d}.parquet"
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, str(path), compression="snappy")
        logger.info("LOCAL ← %d rows → %s", len(rows), path)
        return str(path)


# ---------------------------------------------------------------------------
# Main extractor
# ---------------------------------------------------------------------------

class MongoDBExtractor:
    """
    Incremental MongoDB → Snowflake extractor.

    Flow prod  : MongoDB Atlas → GCS Parquet → Snowflake staging → dbt
    Flow local : MongoDB Atlas → local Parquet → Snowflake staging → dbt

    Watermark incrémental persisté dans Snowflake (INTERNAL.MONGO_SYNC_STATE).
    GDPR : PII fields (phone, address) hashés SHA-256 avant tout write.
    """

    def __init__(
        self,
        mongo_uri:       str,
        mongo_db:        str,
        sf_conn:         snowflake.connector.SnowflakeConnection,
        sf_schema:       str = "RAW_MONGODB",
        storage_mode:    str = "auto",
        gcs_bucket:      str | None = None,
        local_path:      str = "C:/tmp/bronze",
        processing_date: str | None = None,
    ) -> None:
        self._mongo  = MongoClient(mongo_uri, serverSelectionTimeoutMS=5_000)
        self._db     = self._mongo[mongo_db]
        self._sf     = sf_conn
        self._schema = sf_schema
        self._date   = processing_date or datetime.now().strftime("%Y-%m-%d")

        mode = storage_mode if storage_mode != "auto" else _detect_storage_mode()
        self._storage_mode = mode

        if mode == "gcs":
            bucket = gcs_bucket or os.getenv("GCS_BRONZE_BUCKET", "gs://dataflow-dev-bronze")
            self._writer: GCSWriter | LocalWriter = GCSWriter(bucket=bucket)
        else:
            self._writer = LocalWriter(base_path=local_path)

        logger.info(
            "MongoDBExtractor ready — mode=%s  db=%s  schema=%s  date=%s",
            self._storage_mode, mongo_db, sf_schema, self._date,
        )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self, collections: list[str] | None = None) -> list[ExtractionResult]:
        self._ensure_snowflake_tables()
        targets = collections or ["profiles", "events_meta", "consents"]
        results = []
        for col_name in targets:
            result = self._extract_collection(col_name)
            results.append(result)
            logger.info(
                "✓ %-20s docs=%-8d upserted=%-8d mode=%-6s time=%.1fs",
                col_name, result.docs_extracted, result.docs_upserted,
                result.storage_mode, result.duration_s,
            )
        return results

    # ------------------------------------------------------------------
    # Per-collection
    # ------------------------------------------------------------------

    def _extract_collection(self, collection_name: str) -> ExtractionResult:
        t0         = time.monotonic()
        watermark  = self._get_watermark(collection_name)
        collection = self._db[collection_name]

        docs_extracted = 0
        docs_upserted  = 0
        new_watermark  = watermark
        batch_id       = 0
        output_paths: list[str] = []

        for batch in self._iter_batches(collection, watermark):
            rows = [self._transform(doc, collection_name) for doc in batch]

            # 1. Parquet (GCS ou local)
            path = self._writer.write_batch(rows, collection_name, batch_id, self._date)
            output_paths.append(path)
            batch_id += 1

            # 2. Snowflake upsert
            self._upsert_snowflake(rows, collection_name)

            docs_extracted += len(batch)
            docs_upserted  += len(rows)

            batch_max = max(
                (r["updated_at"] for r in rows if r.get("updated_at")),
                default=str(new_watermark),
            )
            if str(batch_max) > str(new_watermark):
                new_watermark = batch_max

        if docs_extracted > 0:
            self._set_watermark(collection_name, new_watermark)

        return ExtractionResult(
            collection=collection_name,
            docs_extracted=docs_extracted,
            docs_upserted=docs_upserted,
            duration_s=time.monotonic() - t0,
            watermark=new_watermark if isinstance(new_watermark, datetime)
                      else datetime.now(timezone.utc),
            storage_mode=self._storage_mode,
            output_paths=output_paths,
        )

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def _iter_batches(
        self, collection: Collection, watermark: datetime
    ) -> Iterator[list[dict]]:
        cursor = collection.find(
            {"updated_at": {"$gt": watermark}},
            sort=[("updated_at", 1)],
            batch_size=BATCH_SIZE,
            no_cursor_timeout=False,
        )
        batch: list[dict] = []
        try:
            for doc in cursor:
                batch.append(doc)
                if len(batch) >= BATCH_SIZE:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Transform (GDPR + flatten)
    # ------------------------------------------------------------------

    def _transform(self, doc: dict, collection_name: str) -> dict:
        def _serialize(v: Any) -> Any:
            if isinstance(v, ObjectId): return str(v)
            if isinstance(v, datetime): return v.isoformat()
            if isinstance(v, dict):
                return json.dumps({k: _serialize(vv) for k, vv in v.items()})
            if isinstance(v, list):
                return json.dumps([_serialize(i) for i in v])
            return v

        def _hash_pii(value: str | None) -> str | None:
            return hashlib.sha256(value.encode()).hexdigest() if value else None

        KNOWN = {
            "user_id", "email", "first_name", "last_name",
            "country_code", "city", "customer_segment", "loyalty_tier",
            "acquisition_channel", "acquisition_date",
            "preferred_category", "preferred_device",
            "ltv_estimate_eur", "predicted_churn_score",
            "profile_completeness_pct", "last_active_at",
            "phone", "address", "consents", "tags",
            "updated_at", "created_at",
        }

        row: dict[str, Any] = {
            "_id":           str(doc.get("_id", "")),
            "_collection":   collection_name,
            "_extracted_at": datetime.now(timezone.utc).isoformat(),
            "_storage_mode": self._storage_mode,
        }

        for f in KNOWN:
            val = doc.get(f)
            if f in ("phone", "address"):
                row[f"{f}_hash"] = _hash_pii(str(val) if val else None)
            else:
                row[f] = json.dumps(val) if isinstance(val, (list, dict)) else _serialize(val)

        extra = {k: _serialize(v) for k, v in doc.items()
                 if k not in KNOWN and k != "_id"}
        row["_extra"] = json.dumps(extra) if extra else None
        return row

    # ------------------------------------------------------------------
    # Snowflake
    # ------------------------------------------------------------------

    def _ensure_snowflake_tables(self) -> None:
        cur = self._sf.cursor()
        try:
            cur.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE', 'DATAFLOW_DEV')}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(f"USE SCHEMA {self._schema}")

            for col in ("profiles", "events_meta", "consents"):
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS STG_MONGO_{col.upper()} (
                        _id                      VARCHAR,
                        _collection              VARCHAR,
                        _extracted_at            VARCHAR,
                        _storage_mode            VARCHAR,
                        user_id                  VARCHAR,
                        email                    VARCHAR,
                        first_name               VARCHAR,
                        last_name                VARCHAR,
                        country_code             VARCHAR,
                        city                     VARCHAR,
                        customer_segment         VARCHAR,
                        loyalty_tier             VARCHAR,
                        acquisition_channel      VARCHAR,
                        acquisition_date         VARCHAR,
                        preferred_category       VARCHAR,
                        preferred_device         VARCHAR,
                        ltv_estimate_eur         FLOAT,
                        predicted_churn_score    FLOAT,
                        profile_completeness_pct INTEGER,
                        last_active_at           VARCHAR,
                        phone_hash               VARCHAR,
                        address_hash             VARCHAR,
                        consents                 VARIANT,
                        tags                     VARIANT,
                        updated_at               VARCHAR,
                        created_at               VARCHAR,
                        _extra                   VARIANT
                    )
                """)

            cur.execute("CREATE SCHEMA IF NOT EXISTS INTERNAL")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS INTERNAL.MONGO_SYNC_STATE (
                    collection_name VARCHAR PRIMARY KEY,
                    last_sync_at    TIMESTAMP_NTZ
                )
            """)
            logger.info("Snowflake tables OK ✓")
        finally:
            cur.close()

    def _upsert_snowflake(self, rows: list[dict], collection_name: str) -> None:
        if not rows:
            return
        table        = f"STG_MONGO_{collection_name.upper()}"
        columns      = list(rows[0].keys())
        variant_cols = {"consents", "tags", "_extra"}
        placeholders = ", ".join(["%s"] * len(columns))
        col_list     = ", ".join(columns)
        cur          = self._sf.cursor()
        try:
            tmp = f"{table}_TMP_{int(time.time())}"
            # Crée la table temp avec tout en VARCHAR
            col_defs = ", ".join([f"{c} VARCHAR" for c in columns])
            cur.execute(f"CREATE TEMP TABLE {tmp} ({col_defs})")
            cur.executemany(
                f"INSERT INTO {tmp} ({col_list}) VALUES ({placeholders})",
                [[row.get(c) for c in columns] for row in rows],
            )
            # MERGE avec PARSE_JSON pour les colonnes VARIANT
            set_clause = ", ".join(
                f"{c} = PARSE_JSON(source.{c})" if c in variant_cols
                else f"{c} = source.{c}"
                for c in columns
            )
            insert_vals = ", ".join(
                f"PARSE_JSON(source.{c})" if c in variant_cols
                else f"source.{c}"
                for c in columns
            )
            cur.execute(f"""
                MERGE INTO {table} AS target
                USING {tmp} AS source ON target._id = source._id
                WHEN MATCHED AND source.updated_at > target.updated_at THEN
                    UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({col_list}) VALUES ({insert_vals})
            """)
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {tmp}")
            cur.close()

    def _get_watermark(self, collection_name: str) -> datetime:
        cur = self._sf.cursor()
        try:
            cur.execute("""
                SELECT last_sync_at FROM INTERNAL.MONGO_SYNC_STATE
                WHERE collection_name = %s
            """, (collection_name,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0].replace(tzinfo=timezone.utc)
        except Exception:
            pass
        finally:
            cur.close()
        return datetime(2020, 1, 1, tzinfo=timezone.utc)

    def _set_watermark(self, collection_name: str, ts: Any) -> None:
        cur = self._sf.cursor()
        try:
            cur.execute("""
                MERGE INTO INTERNAL.MONGO_SYNC_STATE AS t
                USING (SELECT %s AS collection_name, %s::TIMESTAMP_NTZ AS last_sync_at) AS s
                ON t.collection_name = s.collection_name
                WHEN MATCHED     THEN UPDATE SET last_sync_at = s.last_sync_at
                WHEN NOT MATCHED THEN INSERT (collection_name, last_sync_at)
                                      VALUES (s.collection_name, s.last_sync_at)
            """, (collection_name, str(ts)))
        finally:
            cur.close()

    def close(self) -> None:
        self._mongo.close()
        self._sf.close()

    def __enter__(self) -> "MongoDBExtractor":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Seed MongoDB avec données réalistes
# ---------------------------------------------------------------------------

def seed_mongodb(mongo_uri: str, n_users: int = 50_000) -> None:
    import random, uuid
    from datetime import timedelta

    client = MongoClient(mongo_uri)
    db = client["users"]

    existing = db.profiles.count_documents({})
    if existing >= n_users:
        logger.info("MongoDB déjà seedé : %d profils ✓", existing)
        client.close()
        return

    logger.info("Seed MongoDB Atlas : %d profils...", n_users)

    db.profiles.create_index("updated_at")
    db.profiles.create_index("user_id", unique=True)
    db.events_meta.create_index("updated_at")
    db.consents.create_index("updated_at")

    SEGMENTS   = ["VIP", "Loyal", "New", "Lapsed", "At-Risk"]
    CHANNELS   = ["organic_search", "paid_social", "email", "direct", "referral"]
    CATEGORIES = ["chaussures", "vêtements", "accessoires", "sport", "maison"]
    COUNTRIES  = ["FR", "DE", "ES", "GB", "IT", "BE", "NL", "CH"]
    DEVICES    = ["desktop", "mobile", "tablet"]

    profiles = []; events_meta = []; consents = []
    BATCH = 5_000

    for i in range(n_users):
        uid     = str(uuid.uuid4())
        created = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))
        updated = created + timedelta(days=random.randint(0, 365))

        profiles.append({
            "user_id": uid, "email": f"user_{i}@example.com",
            "first_name": random.choice(["Alice","Bob","Clara","David","Emma","Félix"]),
            "last_name":  random.choice(["Martin","Dupont","Schmidt","García","Rossi"]),
            "country_code": random.choice(COUNTRIES),
            "city": random.choice(["Paris","Berlin","Madrid","Rome","London"]),
            "customer_segment": random.choice(SEGMENTS),
            "loyalty_tier": random.choice(["bronze","silver","gold","platinum"]),
            "acquisition_channel": random.choice(CHANNELS),
            "acquisition_date": created.strftime("%Y-%m-%d"),
            "preferred_category": random.choice(CATEGORIES),
            "preferred_device": random.choice(DEVICES),
            "ltv_estimate_eur": round(random.uniform(0, 2500), 2),
            "predicted_churn_score": round(random.uniform(0, 1), 4),
            "profile_completeness_pct": random.randint(40, 100),
            "last_active_at": updated.isoformat(),
            "phone": f"+33 6 {random.randint(10,99)} {random.randint(10,99)} {random.randint(10,99)} {random.randint(10,99)}",
            "tags": random.sample(["vip","soldes","newsletter","loyalty"], k=random.randint(0,3)),
            "created_at": created, "updated_at": updated,
        })
        events_meta.append({
            "user_id": uid,
            "total_sessions": random.randint(1, 500),
            "total_page_views": random.randint(5, 10_000),
            "total_purchases": random.randint(0, 50),
            "total_revenue_eur": round(random.uniform(0, 3000), 2),
            "avg_session_duration_s": random.randint(30, 900),
            "last_seen_at": updated.isoformat(),
            "updated_at": updated, "created_at": created,
        })
        consents.append({
            "user_id": uid,
            "email": random.choice([True, True, False]),
            "sms":   random.choice([True, False, False]),
            "push":  random.choice([True, False]),
            "updated_at": updated, "created_at": created,
        })

        if len(profiles) >= BATCH:
            db.profiles.insert_many(profiles, ordered=False)
            db.events_meta.insert_many(events_meta, ordered=False)
            db.consents.insert_many(consents, ordered=False)
            logger.info("  Inséré %d/%d", min(i + 1, n_users), n_users)
            profiles = []; events_meta = []; consents = []

    if profiles:
        db.profiles.insert_many(profiles, ordered=False)
        db.events_meta.insert_many(events_meta, ordered=False)
        db.consents.insert_many(consents, ordered=False)

    logger.info("✓ Seed terminé — %d profils dans MongoDB Atlas", db.profiles.count_documents({}))
    client.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="MongoDB → Snowflake extractor (GCS ou local fallback)")
    p.add_argument("--collections", nargs="*", help="Collections à extraire (défaut: toutes)")
    p.add_argument("--storage-mode", choices=["auto", "gcs", "local"], default="auto")
    p.add_argument("--seed", action="store_true", help="Force seed MongoDB si vide")
    p.add_argument("--n-users", type=int, default=50_000)
    args = p.parse_args()

    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        raise ValueError("MONGODB_URI manquant dans .env — vérifie ton fichier .env")

    # Seed automatique si collection vide
    client = MongoClient(mongo_uri)
    count  = client["users"].profiles.count_documents({})
    client.close()
    if count == 0 or args.seed:
        seed_mongodb(mongo_uri, n_users=args.n_users)

    # Connexion Snowflake
    sf_conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE", "DATAFLOW_DEV"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
        role      = os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER_ROLE"),
    )

    with MongoDBExtractor(
        mongo_uri       = mongo_uri,
        mongo_db        = "users",
        sf_conn         = sf_conn,
        storage_mode    = args.storage_mode,
        processing_date = os.getenv("PROCESSING_DATE"),
    ) as extractor:
        results = extractor.run(collections=args.collections)

    print("\n=== RÉSUMÉ EXTRACTION ===")
    for r in results:
        print(f"  {r.collection:<20} docs={r.docs_extracted:<8} "
              f"upserted={r.docs_upserted:<8} mode={r.storage_mode:<6} {r.duration_s:.1f}s")
        for path in r.output_paths:
            print(f"    → {path}")