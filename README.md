# 🏗️ DataFlow E-Commerce Data Platform

> Production-grade data engineering platform for behavioral analytics at scale —
> processing **2B+ events/day** across a modern Lakehouse architecture on GCP.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)](https://spark.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Enterprise-29b5e8?logo=snowflake)](https://snowflake.com)
[![GCP](https://img.shields.io/badge/GCP-BigQuery_+_GCS-4285F4?logo=google-cloud)](https://cloud.google.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-GKE-326CE5?logo=kubernetes)](https://kubernetes.io)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis)](https://redis.io)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B)](https://getdbt.com)

---

## 📐 Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES (15+)                                  │
│  Salesforce CRM │ REST APIs (×7) │ PostgreSQL (×3) │ MongoDB │ Kafka Topics  │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │  Airbyte ELT + Custom Connectors
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│               MEDALLION LAKEHOUSE  (GCS + BigQuery)                          │
│                                                                              │
│   🥉 BRONZE              🥈 SILVER                   🥇 GOLD                 │
│   Raw Parquet      →    Cleaned + typed         →   Business aggregates      │
│   (append-only)         (Delta, deduped)             (serving layer)         │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │  PySpark on Databricks (2B events/day)
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE DATA WAREHOUSE  (Star Schema)                   │
│                                                                              │
│          fact_orders ──┬── dim_customers                                     │
│                        ├── dim_products                                      │
│                        └── dim_time                                          │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
               ┌─────────────────┼──────────────────┐
               ▼                 ▼                  ▼
          Redis Cache        Analytics          ML Feature Store
          (query layer)      Dashboards         (Vertex AI)
          <200ms p99         Metabase/Looker
```

---

## 📂 Repository Structure

```
dataflow-ecommerce-platform/
├── ingestion/
│   ├── connectors/              # Custom Python source connectors
│   │   ├── rest_api_connector.py
│   │   └── mongodb_extractor.py
│   ├── airbyte_connections/     # Airbyte source/dest YAML definitions
│   └── schemas/                 # JSON Schema contracts per source
│
├── transforms/
│   ├── bronze/
│   │   └── raw_events_loader.py
│   ├── silver/
│   │   └── clickstream_processor.py   ← PySpark, 2B events/day
│   └── gold/
│       └── daily_sales_aggregator.py
│
├── models/                      # dbt Snowflake models
│   ├── staging/
│   ├── marts/                   # Star schema (fact_orders + dims)
│   └── schema.yml
│
├── orchestration/
│   └── dags/
│       └── ecommerce_daily_pipeline.py   ← Airflow DAG (GKE)
│
├── cache/
│   └── query_cache.py           ← Redis layer (−40% Snowflake load)
│
├── infrastructure/
│   ├── terraform/
│   └── k8s/
│
├── monitoring/
│   └── great_expectations/
│
├── tests/
│   ├── unit/
│   └── integration/
│
└── .github/workflows/
    └── ci.yml
```

---

## 🚀 Quick Start

```bash
# Clone & setup
git clone https://github.com/yourusername/dataflow-ecommerce-platform
cd dataflow-ecommerce-platform
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure secrets
cp .env.example .env

# Run local stack (Airflow + Redis + Postgres meta)
docker-compose up -d

# Submit a Spark Silver job
spark-submit transforms/silver/clickstream_processor.py \
  --input  gs://dataflow-bronze/events/2024-01-15/ \
  --output gs://dataflow-silver/events/ \
  --date   2024-01-15

# Run dbt models
cd models && dbt run --select marts.*

# Trigger Airflow DAG
airflow dags trigger ecommerce_daily_pipeline --conf '{"date":"2024-01-15"}'
```

---

## 📊 Performance Benchmarks

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| New source onboarding | 3 days | < 4 hours | **−95%** |
| Analytics query response | 3.2 s | < 200 ms | **−94%** |
| Snowflake warehouse load | baseline | −40% | **−40%** |
| Analyst report generation | baseline | 5× faster | **+400%** |
| Daily event throughput | — | 2.1B events/day | — |

---

## 🧪 Tests

```bash
pytest tests/unit/        -v --cov=transforms --cov-report=term-missing
pytest tests/integration/ -v -m "not slow"
great_expectations checkpoint run clickstream_silver_checkpoint
```

---

## 🏗️ Infrastructure

- **Compute** : GKE `n2-standard-8`, autoscaling 3 → 20 nodes (Black Friday)
- **Storage** : GCS multi-region (Bronze / Silver / Gold separation)
- **Warehouse** : Snowflake Enterprise — 3 virtual warehouses (ingest / analytics / ml)
- **Cache** : Redis Cluster 3 nodes × 32 GB — ~68% cache hit rate on dashboards
- **Orchestration** : Airflow 2.8 on GKE, CeleryExecutor, 200+ tasks/day

---

*Built at DataFlow Solutions, Paris 🇫🇷 — behavioral analytics platform serving 200+ European e-commerce brands.*
