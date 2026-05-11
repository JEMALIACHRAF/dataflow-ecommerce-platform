# 🏗️ DataFlow E-Commerce Data Platform

> **Production-grade data engineering platform** for behavioral analytics at scale —
> processing **2B+ events/day** across a modern Lakehouse architecture on GCP + Snowflake.
>
> Built to reflect a real-world Data Engineer role at a European e-commerce scale-up.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange?logo=apache-spark)](https://spark.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Enterprise-29b5e8?logo=snowflake)](https://snowflake.com)
[![GCP](https://img.shields.io/badge/GCP-Pub/Sub_+_GCS_+_GKE-4285F4?logo=google-cloud)](https://cloud.google.com)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-FF694B)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.4-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis)](https://redis.io)
[![MongoDB](https://img.shields.io/badge/MongoDB-Atlas_M0-47A248?logo=mongodb)](https://mongodb.com)
[![CI](https://github.com/JEMALIACHRAF/dataflow-ecommerce-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/JEMALIACHRAF/dataflow-ecommerce-platform/actions)

---

## 📋 Table of Contents

1. [Business Context](#-business-context)
2. [System Objectives](#-system-objectives)
3. [Architecture Overview](#-architecture-overview)
4. [Technical Choices](#-technical-choices)
5. [Data Flow & Scenarios](#-data-flow--scenarios)
6. [Repository Structure](#-repository-structure)
7. [Prerequisites](#-prerequisites)
8. [Step-by-Step Setup](#-step-by-step-setup)
9. [Running the Pipeline](#-running-the-pipeline)
10. [Performance Benchmarks](#-performance-benchmarks)
11. [Infrastructure](#-infrastructure)
12. [CI/CD](#-cicd)

---

## 🏢 Business Context

**DataFlow Solutions** is a Paris-based scale-up providing behavioral analytics
to 200+ European e-commerce brands. The data team (8 engineers) was tasked with
modernising a fragmented data infrastructure:

| Problem | Before | After |
|---------|--------|-------|
| New data source onboarding | 3 days manual work | < 4 hours (Airbyte) |
| Analytics query response | 3.2s average | < 200ms (Redis cache) |
| Snowflake warehouse load | baseline | −40% |
| Analyst report generation | ad-hoc SQL | 5× faster (dbt star schema) |
| Daily event throughput | batch only | 2.1B events/day (streaming) |

---

## 🎯 System Objectives

```
1. INGEST     — Collect data from 15+ heterogeneous sources reliably
2. PROCESS    — Transform 2B+ daily events with quality guarantees
3. SERVE      — Deliver sub-200ms analytics to dashboards
4. ORCHESTRATE — Run the full pipeline automatically every day
5. OBSERVE    — Detect data quality issues before analysts do
```

---

## 📐 Architecture Overview

### Functional Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES  (15+ sources)                          │
│                                                                             │
│  👤 User Profiles    🛒 Orders        📊 CRM           📡 Clickstream       │
│  MongoDB Atlas       PostgreSQL ×3   Salesforce       GCP Pub/Sub          │
│  (JSON documents)    (transactional) (accounts)       (2B events/day)      │
└──────────────┬───────────────┬──────────────┬──────────────┬───────────────┘
               │               │              │              │
        hourly sync      Airbyte ELT    Airbyte ELT   real-time stream
               │               │              │              │
               ▼               ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MEDALLION LAKEHOUSE  (GCS + BigQuery)                    │
│                                                                             │
│  🥉 BRONZE                🥈 SILVER                  🥇 GOLD               │
│  Raw Parquet          Cleaned + typed           Business aggregates        │
│  Append-only          Deduplicated              Serving layer              │
│  gs://...-bronze/     gs://...-silver/          gs://...-gold/            │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    PySpark on Databricks
                    (Silver + Gold jobs)
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  SNOWFLAKE DATA WAREHOUSE  (Star Schema)                    │
│                                                                             │
│     fact_orders ──┬── dim_customers  (50K customers, SCD Type 2)           │
│                   ├── dim_products                                          │
│                   └── dim_time       (2020→2030, Black Friday flags)       │
│                                                                             │
│     RAW_MONGODB  ←── mongodb_extractor.py  (incremental watermark)         │
│     RAW_SALESFORCE ← Airbyte (6h sync)                                     │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
          Redis Cache      Analytics       ML Feature Store
          < 200ms p99      Dashboards      (Vertex AI)
          −40% SF load     Metabase
```

### Technical Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION LAYER  —  Apache Airflow 2.8 on GKE                         │
│                                                                             │
│  DAG: ecommerce_daily_pipeline  (cron: 0 3 * * *)                         │
│                                                                             │
│  [check_landing] → [bronze_ingest×3] → [silver×3] → [gold] →              │
│  [dbt_staging] → [dbt_marts] → [dbt_test] → [cache_invalidate] →          │
│  [dq_checks] → [notify_slack]                                              │
│                                                                             │
│  GKE: n2-standard-2 | HPA: 1→3 nodes | Workers: 1→20 pods                │
└─────────────────────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌──────────────┐   ┌──────────────────┐   ┌──────────────────────┐
│   INGESTION  │   │   TRANSFORMS     │   │   SERVING            │
│              │   │                  │   │                      │
│ Airbyte ELT  │   │ PySpark 3.5.1    │   │ Redis 7.2            │
│ 15+ sources  │   │ Databricks       │   │ allkeys-lru          │
│              │   │                  │   │ 256MB / 32GB (prod)  │
│ mongodb_     │   │ clickstream_     │   │                      │
│ extractor.py │   │ processor.py     │   │ query_cache.py       │
│              │   │ (Bronze→Silver)  │   │ msgpack + lz4        │
│ pubsub_      │   │                  │   │ tag invalidation     │
│ producer.py  │   │ daily_sales_     │   │ ~68% hit rate        │
│              │   │ aggregator.py    │   │                      │
│ pubsub_      │   │ (Silver→Gold)    │   │ < 200ms p99          │
│ consumer.py  │   │                  │   │                      │
└──────────────┘   └──────────────────┘   └──────────────────────┘
          │                    │
          ▼                    ▼
┌──────────────────────────────────────────┐
│   DATA QUALITY  —  Great Expectations    │
│                                          │
│ clickstream_silver_checkpoint.py         │
│ 15+ expectations per layer:              │
│  ✓ Volume guard (500M→5B rows/day)       │
│  ✓ Uniqueness (event_id)                 │
│  ✓ Freshness (< 8h)                     │
│  ✓ GDPR (no raw IP, phone hashed)        │
└──────────────────────────────────────────┘
```

---

## 🔧 Technical Choices

### Why these technologies?

| Technology | Role | Why chosen |
|-----------|------|-----------|
| **GCP Pub/Sub** | Event streaming | Managed Kafka equivalent — no cluster to maintain, auto-scales to millions/sec |
| **GCS (Bronze/Silver/Gold)** | Lakehouse storage | Medallion architecture — raw data preserved, transformations traceable |
| **PySpark on Databricks** | Batch processing | 2B events/day requires distributed computing; Databricks = managed Spark |
| **Snowflake** | Analytics warehouse | Columnar storage, virtual warehouses, perfect for complex JOIN queries |
| **dbt** | SQL transformations | Version-controlled SQL, auto-generated docs, built-in tests |
| **MongoDB Atlas** | User profiles | JSON flexibility for nested structures (consents, tags, preferences) |
| **Redis** | Query cache | −40% Snowflake load, <200ms dashboard response vs 3.2s |
| **Airflow on GKE** | Orchestration | DAG-based pipeline with retry, alerting, and auto-scaling |
| **Great Expectations** | Data quality | Catches schema drift and volume anomalies before analysts |

### Key Design Decisions

**1. Medallion Architecture (Bronze/Silver/Gold)**
```
Bronze = raw data, never modified (audit trail, replay)
Silver = cleaned, typed, GDPR-compliant (production-safe)
Gold   = business aggregations (dashboard-ready)
```

**2. Incremental extraction (watermark pattern)**
```python
# Only process records updated since last run
cursor = collection.find({"updated_at": {"$gt": watermark}})
```
Reduces MongoDB → Snowflake sync from full-scan to delta only.

**3. GDPR by design**
```python
# IP anonymised in Silver — raw IP never leaves Bronze
ip_anonymized = regexp_replace(ip_address, r"\.\d+$", ".0")
# Phone hashed with SHA-256 — never stored in plain text
phone_hash = SHA256(phone)
```

**4. Idempotent upserts (safe reruns)**
```sql
MERGE INTO target USING source ON target._id = source._id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

**5. GCS vs local fallback (portfolio flexibility)**
```python
STORAGE_MODE=auto   # tries GCS, falls back to local automatically
STORAGE_MODE=gcs    # force GCS (requires billing)
STORAGE_MODE=local  # force local (no cloud needed)
```

---

## 🔄 Data Flow & Scenarios

### Scenario 1 — Daily User Profile Sync (MongoDB → Snowflake)

```
00:00 UTC  MongoDB Atlas updated by product backend
           (new registrations, consent updates, ML scores)
           ↓
03:00 UTC  Airflow triggers ecommerce_daily_pipeline
           ↓
03:05 UTC  mongodb_extractor.py runs
           • reads only records with updated_at > last_watermark
           • hashes PII (phone → SHA-256)
           • writes Parquet to gs://dataflow-dev-bronze/profiles/
           • upserts into Snowflake RAW_MONGODB.STG_MONGO_PROFILES
           ↓
03:15 UTC  dbt runs: stg_mongo_profiles → dim_customers
           • customer_sk = MD5(user_id)
           • churn_risk computed from predicted_churn_score
           • is_high_value flag (LTV > 1000€)
           ↓
03:20 UTC  Redis cache invalidated for dim_customers tags
           ↓
03:25 UTC  Dashboard queries hit Redis cache → < 200ms response
```

### Scenario 2 — Real-time Clickstream (Pub/Sub → GCS → Spark)

```
Continuous  User navigates shop.dataflow.io
            Frontend SDK publishes event to Pub/Sub:
            {"event_type": "product_view", "product_id": "SKU-0042", ...}
            ↓
            pubsub_clickstream_producer.py publishes ~200 events/sec
            ↓
Every hour  pubsub_spark_consumer.py pulls from subscription
            • batches of 1000 events
            • writes Parquet to gs://dataflow-dev-bronze/clickstream/
            ↓
            clickstream_processor.py (PySpark Silver job):
            • parses timestamps (ISO, Unix-ms, Unix-s)
            • anonymises IPs (GDPR)
            • deduplicates on event_id
            • classifies referrer_channel
            • normalises revenue (€/$, FR locale comma)
            • writes Delta to gs://dataflow-dev-silver/clickstream/
            ↓
            daily_sales_aggregator.py (PySpark Gold):
            • gold_daily_sales (revenue by country/channel)
            • gold_funnel_metrics (view→cart→checkout→purchase)
            • gold_product_performance (top products)
            • writes to BigQuery + Snowflake MARTS
```

### Scenario 3 — Black Friday Auto-scaling

```
Normal day    GKE: 1 node, 1 Airflow worker pod
              Pub/Sub: ~200 events/sec
              Redis: ~68% cache hit rate

Black Friday  Traffic spike detected (CPU > 70%)
              ↓
              HPA scales Airflow workers: 1 → 20 pods
              GKE Cluster Autoscaler: 1 → 3 nodes
              ↓
              Pub/Sub absorbs spike (unlimited throughput)
              ↓
              Spark job processes backlog in parallel
              ↓
              After peak: auto-scale back to 1 node (cost savings)
```

---

## 📂 Repository Structure

```
dataflow-ecommerce-platform/
│
├── ingestion/
│   ├── connectors/
│   │   ├── mongodb_extractor.py      # MongoDB → Snowflake (incremental, GDPR)
│   │   ├── rest_api_connector.py     # Generic REST API → GCS Bronze
│   │   └── pubsub_clickstream_producer.py  # Pub/Sub event publisher
│   ├── airbyte_connections/
│   │   ├── salesforce_crm.yaml       # Salesforce → Snowflake (6h sync)
│   │   └── mongodb_user_profiles.yaml # MongoDB → Snowflake (1h sync)
│   └── schemas/
│       └── clickstream_event_v2.json  # JSON Schema contract
│
├── transforms/
│   ├── bronze/
│   │   └── raw_events_loader.py      # Generic Bronze loader
│   ├── silver/
│   │   ├── clickstream_processor.py  # PySpark: Bronze → Silver (2B/day)
│   │   └── pubsub_spark_consumer.py  # Pub/Sub consumer → GCS Bronze
│   └── gold/
│       └── daily_sales_aggregator.py # PySpark: Silver → Gold aggregations
│
├── models/                           # dbt project (Snowflake star schema)
│   └── dataflow_ecommerce/
│       └── models/
│           ├── staging/
│           │   └── stg_mongo_profiles.sql
│           └── marts/
│               ├── dim_customers.sql  # SCD Type 2, churn_risk, is_high_value
│               ├── dim_time.sql       # 2020→2030, Black Friday, soldes FR
│               └── fact_orders.sql    # Central fact table, ~150K orders
│
├── orchestration/
│   └── dags/
│       └── ecommerce_daily_pipeline.py  # Airflow DAG (GKE, CeleryExecutor)
│
├── cache/
│   └── query_cache.py               # Redis cache (msgpack+lz4, tag invalidation)
│
├── scripts/
│   └── databricks_deploy.py         # Auto-deploy notebooks to Databricks via API
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf                  # GKE, GCS, BigQuery, Redis, IAM
│   │   └── variables.tf
│   └── k8s/
│       ├── airflow-deployment.yaml  # Webserver + Scheduler + Workers + HPA
│       ├── airflow-pvc.yaml         # PersistentVolumeClaim for DAGs
│       └── airflow-sa.yaml          # ServiceAccount + RBAC
│
├── monitoring/
│   └── great_expectations/
│       └── clickstream_silver_checkpoint.py  # 15+ data quality expectations
│
├── tests/
│   ├── unit/
│   │   ├── test_clickstream_processor.py  # PySpark unit tests (local Spark)
│   │   └── test_query_cache.py            # Redis cache tests (fakeredis)
│   └── integration/
│       └── test_pipeline_end_to_end.py    # Full pipeline integration tests
│
├── .github/
│   └── workflows/
│       └── ci.yml                   # CI/CD: lint → unit tests → dbt → deploy
│
├── .env.example                     # Environment variables template
├── requirements.txt                 # Python dependencies (local dev)
├── requirements-cloud.txt           # Cloud dependencies (GKE workers)
└── conftest.py                      # pytest PYTHONPATH configuration
```

---

## 🔑 Prerequisites

### Cloud Accounts Required

| Service | Plan | Cost | Link |
|---------|------|------|------|
| **Snowflake** | Trial (30 days, $400 credits) | Free | [trial.snowflake.com](https://trial.snowflake.com) |
| **MongoDB Atlas** | M0 Free cluster | Free forever | [cloud.mongodb.com](https://cloud.mongodb.com) |
| **GCP** | Free tier + billing enabled | ~$0.05/day | [console.cloud.google.com](https://console.cloud.google.com) |
| **Azure Databricks** | Trial or existing workspace | Free trial | [azure.microsoft.com](https://azure.microsoft.com) |

### Local Tools Required

```bash
# Verify installations
python --version      # 3.11+
java -version         # 11 or 17 (required for PySpark)
docker --version      # 20+
gcloud --version      # Google Cloud SDK
git --version         # 2+
```

---

## 🚀 Step-by-Step Setup

### Step 1 — Clone & Python Environment

```bash
git clone https://github.com/JEMALIACHRAF/dataflow-ecommerce-platform.git
cd dataflow-ecommerce-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\Activate.ps1      # Windows PowerShell

# Install dependencies
pip install -r requirements.txt
```

### Step 2 — Configure Snowflake

```sql
-- Run in Snowflake Worksheet (ACCOUNTADMIN role)
CREATE DATABASE DATAFLOW_DEV;
CREATE SCHEMA DATAFLOW_DEV.RAW_MONGODB;
CREATE SCHEMA DATAFLOW_DEV.RAW_SALESFORCE;
CREATE SCHEMA DATAFLOW_DEV.SILVER;
CREATE SCHEMA DATAFLOW_DEV.MARTS;
CREATE SCHEMA DATAFLOW_DEV.INTERNAL;

CREATE WAREHOUSE INGEST_WH    WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE WAREHOUSE ANALYTICS_WH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;

CREATE ROLE TRANSFORMER_ROLE;
GRANT ALL ON DATABASE DATAFLOW_DEV TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON WAREHOUSE INGEST_WH TO ROLE TRANSFORMER_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE DATAFLOW_DEV TO ROLE TRANSFORMER_ROLE;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE DATAFLOW_DEV TO ROLE TRANSFORMER_ROLE;
GRANT ROLE TRANSFORMER_ROLE TO USER <your_username>;

-- Find your account identifier
SELECT CURRENT_ACCOUNT();  -- e.g. CV69340
-- Region: check Admin > Accounts (e.g. eu-west-3.aws for Paris)
```

### Step 3 — Configure MongoDB Atlas

1. Go to [cloud.mongodb.com](https://cloud.mongodb.com)
2. Create cluster → **M0 Free** → AWS Frankfurt
3. Database Access → Add user `dataflow_user` / `DataFlow2024!`
4. Network Access → Allow `0.0.0.0/0`
5. Connect → Drivers → copy connection string

### Step 4 — Configure GCP

```bash
# Authenticate
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Enable APIs
gcloud services enable pubsub.googleapis.com storage.googleapis.com bigquery.googleapis.com

# Create service account
gcloud iam service-accounts create dataflow-dev-sa --display-name="DataFlow Dev SA"
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:dataflow-dev-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:dataflow-dev-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:dataflow-dev-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# Download key
gcloud iam service-accounts keys create gcp-key.json \
  --iam-account="dataflow-dev-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com"

# Create GCS buckets
gcloud storage buckets create gs://dataflow-dev-bronze --location=EU --uniform-bucket-level-access
gcloud storage buckets create gs://dataflow-dev-silver --location=EU --uniform-bucket-level-access
gcloud storage buckets create gs://dataflow-dev-gold   --location=EU --uniform-bucket-level-access

# Create Pub/Sub topic
gcloud pubsub topics create clickstream-events
gcloud pubsub subscriptions create clickstream-events-sub --topic=clickstream-events
```

### Step 5 — Configure .env

```bash
cp .env.example .env
# Edit .env with your credentials (never commit this file)
```

```ini
# GCP
GCP_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-key.json
GCS_BRONZE_BUCKET=gs://dataflow-dev-bronze
GCS_SILVER_BUCKET=gs://dataflow-dev-silver
GCS_GOLD_BUCKET=gs://dataflow-dev-gold
STORAGE_MODE=auto

# Snowflake
SNOWFLAKE_ACCOUNT=XXXXXXX.eu-west-3.aws
SNOWFLAKE_USER=YOUR_USER
SNOWFLAKE_PASSWORD=YOUR_PASSWORD
SNOWFLAKE_DATABASE=DATAFLOW_DEV
SNOWFLAKE_WAREHOUSE=ANALYTICS_WH
SNOWFLAKE_ROLE=TRANSFORMER_ROLE

# MongoDB Atlas
MONGODB_URI=mongodb+srv://dataflow_user:PASSWORD@cluster0.xxxxx.mongodb.net/users

# Redis (Docker local)
REDIS_URL=redis://localhost:6379/0

# Databricks
DATABRICKS_TOKEN=your-databricks-pat-token

# Pub/Sub
PROCESSING_DATE=2024-01-15
```

### Step 6 — Configure dbt

```bash
cd models/dataflow_ecommerce
dbt init dataflow_ecommerce   # follow prompts for Snowflake
dbt debug                     # verify connection
```

---

## ▶️ Running the Pipeline

### 1. MongoDB → Snowflake (User Profiles)

```bash
# Seeds 50K profiles in MongoDB Atlas + extracts to Snowflake
python ingestion/connectors/mongodb_extractor.py

# Verify in Snowflake:
# SELECT COUNT(*) FROM DATAFLOW_DEV.RAW_MONGODB.STG_MONGO_PROFILES;
```

### 2. dbt Star Schema

```bash
cd models/dataflow_ecommerce
dbt run    # builds all models
dbt test   # runs 32 data quality tests
dbt docs generate && dbt docs serve  # interactive DAG at localhost:8080
```

### 3. Clickstream Streaming (Pub/Sub → GCS → Spark)

```bash
# Terminal 1: publish events to Pub/Sub
python ingestion/connectors/pubsub_clickstream_producer.py --n-events 10000

# Terminal 2: consume and write to GCS Bronze
python transforms/silver/pubsub_spark_consumer.py \
  --bronze-path gs://dataflow-dev-bronze/clickstream \
  --skip-spark   # skip local Spark, use Databricks instead

# Auto-deploy Silver job to Databricks and run it
python scripts/databricks_deploy.py
```

### 4. Redis Cache

```bash
# Start Redis
docker run -d --name redis-dataflow -p 6380:6379 \
  redis:7.2-alpine redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

# Test cache
python test_cache.py
```

### 5. Airflow DAG

```bash
# Copy DAG to running Airflow instance
docker cp orchestration/dags/ecommerce_daily_pipeline.py \
  <airflow-container>:/opt/airflow/dags/

# Trigger manually
docker exec <airflow-container> airflow dags trigger ecommerce_daily_pipeline
```

### 6. Run Tests

```bash
pytest tests/unit/ -v --cov=transforms --cov=cache --cov-report=term-missing
pytest tests/integration/ -v -m "not slow"
```

### 7. Deploy GKE Infrastructure (optional)

```bash
# Requires GCP billing enabled
gcloud container clusters create dataflow-dev \
  --zone=europe-west9-a \
  --num-nodes=1 \
  --machine-type=e2-standard-2 \
  --disk-type=pd-standard \
  --disk-size=50

gcloud container clusters get-credentials dataflow-dev --zone=europe-west9-a
kubectl apply -f infrastructure/k8s/airflow-sa.yaml
kubectl apply -f infrastructure/k8s/airflow-pvc.yaml
kubectl apply -f infrastructure/k8s/airflow-deployment.yaml
kubectl get pods -n airflow

# IMPORTANT: delete cluster when done to avoid costs
gcloud container clusters delete dataflow-dev --zone=europe-west9-a --quiet
```

---

## 📊 Performance Benchmarks

| Metric | Value | Notes |
|--------|-------|-------|
| MongoDB seed (50K profiles) | ~13 min | M0 free tier, network latency |
| MongoDB extraction (150K docs) | ~6 min | Incremental, GDPR transform |
| dbt full run (4 models) | ~10 sec | Snowflake X-Small warehouse |
| dbt test suite (32 tests) | ~14 sec | All parallel |
| Pub/Sub publish (10K events) | ~44 sec | ~227 events/sec |
| Pub/Sub consume → GCS (10K) | ~30 sec | 10 Parquet files |
| Redis cache SET/GET | < 1ms | msgpack + lz4 |
| Redis hit rate (dashboards) | ~68% | allkeys-lru eviction |
| Snowflake query (50K rows) | < 600ms | X-Small warehouse |

**Production targets (2B events/day):**
- Spark Silver job: < 15 min end-to-end
- Snowflake dashboard query: < 200ms (with Redis cache)
- Pipeline SLA: 99.5% uptime

---

## 🏗️ Infrastructure

### GCP Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `dataflow-dev-bronze` | GCS Bucket (EU) | Raw Parquet — append only |
| `dataflow-dev-silver` | GCS Bucket (EU) | Cleaned Delta data |
| `dataflow-dev-gold` | GCS Bucket (EU) | Business aggregations |
| `clickstream-events` | Pub/Sub Topic | Real-time event stream |
| `clickstream-events-sub` | Pub/Sub Subscription | Spark consumer |
| `dataflow-dev` | GKE Cluster (europe-west9-a) | Airflow orchestration |
| `dataflow-dev-sa` | Service Account | IAM least-privilege |

### Snowflake Resources

| Resource | Type |
|----------|------|
| `DATAFLOW_DEV` | Database |
| `RAW_MONGODB`, `RAW_SALESFORCE`, `SILVER`, `MARTS`, `INTERNAL` | Schemas |
| `INGEST_WH`, `ANALYTICS_WH` | Virtual Warehouses (X-Small) |
| `TRANSFORMER_ROLE` | Role (least-privilege) |

---

## 🔄 CI/CD

Pipeline runs on every push to `main` or `develop`:

```
push to main
     │
     ▼
[Unit Tests]          pytest tests/unit/ --cov
     │ PASS
     ▼
[dbt Tests]           dbt compile + dbt test (Snowflake CI schema)
     │ PASS
     ▼
[Validate Configs]    YAML + JSON schema validation
     │ PASS
     ▼
[Deploy]              dbt run --select marts.* (main branch only)
     │
     ▼
[Notify Slack]        ✅ success / 🔴 failure
```

Secrets stored in GitHub Actions (never in code):
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`, `GCP_SA_KEY`

---

## 👤 Author

**Achraf Jemali** — Data & AI Engineer. 
Paris, France 🇫🇷  
[![GitHub](https://img.shields.io/badge/GitHub-JEMALIACHRAF-black?logo=github&style=flat-square)](https://github.com/JEMALIACHRAF)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Achraf_Jemali-0077B5?logo=linkedin&style=flat-square)](https://linkedin.com/in/achraf-jemali-54a417239)



---
