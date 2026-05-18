# 🏗️ DataFlow E-Commerce Data Platform

> Pipeline de data engineering end-to-end simulant l'infrastructure analytique
> d'un e-commerce européen — architecture Medallion sur GCP + Snowflake,
> conçue pour passer à l'échelle et validée sur des volumes de développement.
>
> Objectif : démontrer les patterns de production (idempotence, RGPD by design,
> qualité des données, orchestration) sur un stack moderne complet.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange?logo=apache-spark)](https://spark.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Free_Trial-29b5e8?logo=snowflake)](https://snowflake.com)
[![GCP](https://img.shields.io/badge/GCP-Pub/Sub_+_GCS_+_GKE-4285F4?logo=google-cloud)](https://cloud.google.com)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-FF694B)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.4-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis)](https://redis.io)
[![MongoDB](https://img.shields.io/badge/MongoDB-Atlas_M0-47A248?logo=mongodb)](https://mongodb.com)
[![CI](https://github.com/JEMALIACHRAF/dataflow-ecommerce-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/JEMALIACHRAF/dataflow-ecommerce-platform/actions)

---

## 📋 Table of Contents

1. [Contexte](#-contexte)
2. [Ce que le projet démontre](#-ce-que-le-projet-démontre)
3. [Architecture](#-architecture)
4. [Choix techniques](#-choix-techniques)
5. [Flux de données](#-flux-de-données)
6. [Structure du repo](#-structure-du-repo)
7. [Prérequis](#-prérequis)
8. [Installation](#-installation)
9. [Exécution](#-exécution)
10. [Benchmarks mesurés](#-benchmarks-mesurés)
11. [Infrastructure](#-infrastructure)
12. [CI/CD](#-cicd)

---

## 🏢 Contexte

Ce projet simule la plateforme analytique d'un e-commerce européen multi-pays
(FR, DE, ES, GB...). Il est **entièrement exécutable en environnement de développement**
avec des comptes gratuits (MongoDB Atlas M0, Snowflake Trial, GCP Free Tier).

Le pipeline traite des **données synthétiques réalistes** : 50 000 profils utilisateurs,
~150 000 commandes générées, et des flux d'événements clickstream publiés en batch
via GCP Pub/Sub. L'accent est mis sur la **solidité des patterns** plutôt que sur le volume.

| Composant testé | Volume dev | Pattern démontré |
|----------------|-----------|-----------------|
| Profils MongoDB | 50 000 docs | Extraction incrémentale watermark |
| Commandes Snowflake | ~150 000 lignes | Star schema SCD Type 2 |
| Événements clickstream | 10 000 / run | Déduplication, RGPD, parsing multi-format |
| Cache Redis | requêtes synchrones | Tag invalidation, hit rate ~68% |
| Spark local | 10M events (smoke test) | Pipeline Bronze→Silver en < 60s sur 2 workers |

---

## 🎯 Ce que le projet démontre

**Ingestion**
- Extraction incrémentale MongoDB avec watermark persisté dans Snowflake (pas de full-scan)
- Connecteur REST générique : 4 stratégies de pagination, 3 modes d'auth, rate limiting token-bucket
- Publication d'events via GCP Pub/Sub (~230 events/sec mesurés en dev)

**Transformation (PySpark)**
- Pipeline Bronze → Silver : parsing de timestamps en 5 formats, anonymisation IP (RGPD Art. 25),
  déduplication idempotente first-write-wins, classification de canal referrer, normalisation de devise
- Écriture Delta avec `replaceWhere` — réexécutions quotidiennes sûres sans réécrire les partitions historiques
- Pipeline Silver → Gold : 4 tables d'agrégation (funnel, ventes, produits, sessions), double write BigQuery + Snowflake

**Modélisation (dbt)**
- Star schema Snowflake : `fact_orders`, `dim_customers` (SCD Type 2), `dim_products`, `dim_time`
- Staging multi-source : normalisation des statuts FR/DE/ES, déduplication Salesforce, pseudonymisation RGPD
- 32 tests dbt (unicité, nullité, plages de valeurs, intégrité référentielle)

**Serving**
- Cache Redis read-through avec sérialisation msgpack + compression lz4
- Invalidation par tags après chaque run pipeline
- Décorateur `@cache.cached()` pour cacher les résultats de fonctions analytiques

**Orchestration**
- DAG Airflow complet : sensor GCS → ShortCircuit volumétrie → Silver parallèle → gate qualité SQL → Gold → dbt → vacuum + invalidation cache → Slack
- Retry exponentiel, SLA 4h, `max_active_runs=1`

**Qualité des données**
- Great Expectations : 15+ expectations (schéma, complétude, unicité, validité, fraîcheur, volume)
- Gate qualité SQL dans le DAG Airflow avant Gold
- Alertes Slack en cas d'échec critique

**Infrastructure as Code**
- Terraform GCP : buckets GCS Medallion avec lifecycle rules, GKE (2 node pools avec autoscaling), Redis Memorystore, Secret Manager, IAM least-privilege
- Kubernetes : Airflow sur GKE avec HPA (1 → 20 workers), Workload Identity (pas de fichiers de clé)

**Tests**
- Tests unitaires PySpark avec SparkSession locale (pas de dépendance cloud)
- Tests Redis avec fakeredis (pas de Redis réel nécessaire)
- Tests d'intégration Bronze → Silver en local avec `tmp_path`
- Smoke test : 10M events générés par Spark range processés en < 60s (local[2])

---

## 📐 Architecture

### Vue fonctionnelle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                                   │
│                                                                             │
│  👤 User Profiles    🛒 Orders        📊 CRM           📡 Clickstream       │
│  MongoDB Atlas       PostgreSQL ×3   Salesforce       GCP Pub/Sub          │
│  (JSON documents)    (transactional) (accounts)       (événements batch)   │
└──────────────┬───────────────┬──────────────┬──────────────┬───────────────┘
               │               │              │              │
        sync incrémentale  Airbyte ELT   Airbyte ELT   batch Pub/Sub
               │               │              │              │
               ▼               ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MEDALLION LAKEHOUSE  (GCS + BigQuery)                    │
│                                                                             │
│  🥉 BRONZE                🥈 SILVER                  🥇 GOLD               │
│  Raw Parquet          Cleaned + typed           Agrégations métier         │
│  Append-only          Deduplicated              Serving layer              │
│  gs://...-bronze/     gs://...-silver/          gs://...-gold/            │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                    PySpark (local dev / Databricks prod)
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  SNOWFLAKE DATA WAREHOUSE  (Star Schema)                    │
│                                                                             │
│     fact_orders ──┬── dim_customers  (SCD Type 2, churn_risk)              │
│                   ├── dim_products   (SCD Type 2, margin rate)             │
│                   └── dim_time       (2020→2030, jours fériés FR)          │
│                                                                             │
│     RAW_MONGODB  ←── mongodb_extractor.py  (watermark incrémental)         │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
          Redis Cache      Dashboards      ML Feature Store
          < 200ms (dev)    Metabase        (Vertex AI — design)
          ~68% hit rate
```

### Vue technique

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION — Apache Airflow 2.8 (local Docker / GKE prod)              │
│                                                                             │
│  DAG: ecommerce_daily_pipeline  (cron: 0 2 * * *)                         │
│                                                                             │
│  [sensor_gcs] → [check_volume] → [silver×3] → [quality_gate] →            │
│  [gold×4] → [dbt_staging] → [dbt_marts] → [dbt_test] →                    │
│  [vacuum + cache_invalidate] → [notify_slack]                              │
└─────────────────────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌──────────────┐   ┌──────────────────┐   ┌──────────────────────┐
│   INGESTION  │   │   TRANSFORMS     │   │   SERVING            │
│              │   │                  │   │                      │
│ mongodb_     │   │ clickstream_     │   │ query_cache.py       │
│ extractor.py │   │ processor.py     │   │ msgpack + lz4        │
│              │   │ Bronze→Silver    │   │ tag invalidation     │
│ pubsub_      │   │                  │   │ ~68% hit rate (dev)  │
│ producer.py  │   │ daily_sales_     │   │                      │
│              │   │ aggregator.py    │   │ Redis 7.2            │
│ rest_api_    │   │ Silver→Gold      │   │ 256MB dev / 32GB prod│
│ connector.py │   │                  │   │                      │
└──────────────┘   └──────────────────┘   └──────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────┐
│   DATA QUALITY  —  Great Expectations    │
│                                          │
│ clickstream_silver_checkpoint.py         │
│ 15+ expectations :                       │
│  ✓ Complétude  (event_id, event_ts)      │
│  ✓ Unicité     (event_id)                │
│  ✓ Validité    (event_type, country)     │
│  ✓ Fraîcheur   (< 8h)                   │
│  ✓ RGPD        (IP anonymisée, PII haché)│
│  ✓ Volume      (garde-fou min/max)       │
└──────────────────────────────────────────┘
```

---

## 🔧 Choix techniques

| Technologie | Rôle | Pourquoi |
|------------|------|----------|
| **GCP Pub/Sub** | Streaming d'events | Équivalent Kafka managé — pas de cluster à maintenir, autoscale natif |
| **GCS (Bronze/Silver/Gold)** | Stockage Lakehouse | Architecture Medallion — données brutes préservées, transformations traçables |
| **PySpark** | Traitement batch | Modèle distribué adapté au scale-out ; local en dev, Databricks/Dataproc en prod |
| **Snowflake** | Entrepôt analytique | Stockage colonnaire, virtual warehouses, excellent pour les JOINs complexes |
| **dbt** | Transformations SQL | SQL versionné, docs auto-générés, tests intégrés |
| **MongoDB Atlas** | Profils utilisateurs | Flexibilité JSON pour les structures imbriquées (consentements, tags, préférences) |
| **Redis** | Cache requêtes | Réduit la charge Snowflake, accélère les dashboards (mesuré en dev) |
| **Airflow sur GKE** | Orchestration | DAG avec retry, alerting, autoscaling — Terraform-provisionné |
| **Great Expectations** | Qualité des données | Détecte les dérives de schéma et anomalies de volume avant les analystes |

### Décisions d'architecture clés

**1. Architecture Medallion (Bronze/Silver/Gold)**
```
Bronze = données brutes, jamais modifiées (audit, replay)
Silver = nettoyées, typées, conformes RGPD (prêtes pour la prod)
Gold   = agrégations métier (prêtes pour les dashboards)
```

**2. Extraction incrémentale (watermark pattern)**
```python
# Ne traite que les enregistrements mis à jour depuis le dernier run
cursor = collection.find({"updated_at": {"$gt": watermark}})
# Watermark persisté dans Snowflake → résiste aux redémarrages
```

**3. RGPD by design**
```python
# IP anonymisée dans Silver — l'IP brute ne sort jamais du Bronze
ip_anonymized = regexp_replace(ip_address, r"\.\d+$", ".0")
# Téléphone haché SHA-256 — jamais stocké en clair
phone_hash = SHA256(phone)
```

**4. Idempotence (réexécutions sûres)**
```sql
-- MERGE → pas de doublon si le job tourne deux fois sur le même batch
MERGE INTO target USING source ON target._id = source._id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```
```python
# replaceWhere Delta → réécrit uniquement la partition du jour, pas l'historique
.option("replaceWhere", f"_processing_date = '{processing_date}'")
```

**5. Portabilité dev/prod (fallback GCS ↔ local)**
```bash
STORAGE_MODE=auto   # tente GCS, fallback local si billing non activé
STORAGE_MODE=gcs    # force GCS (nécessite billing GCP)
STORAGE_MODE=local  # force local (aucun cloud requis)
```

---

## 🔄 Flux de données

### Scénario 1 — Sync quotidienne des profils (MongoDB → Snowflake)

```
00:00 UTC  MongoDB Atlas mis à jour par le backend produit
           (nouvelles inscriptions, mises à jour consentements, scores ML)
           ↓
02:00 UTC  Airflow déclenche ecommerce_daily_pipeline
           ↓
02:05 UTC  mongodb_extractor.py s'exécute
           • lit uniquement les docs avec updated_at > last_watermark
           • hache les PII (téléphone → SHA-256)
           • écrit Parquet dans gs://dataflow-dev-bronze/profiles/
           • upsert dans Snowflake RAW_MONGODB.STG_MONGO_PROFILES
           ↓
02:15 UTC  dbt : stg_mongo_profiles → dim_customers
           • customer_sk = MD5(user_id)
           • churn_risk calculé depuis predicted_churn_score
           • is_high_value (LTV > 1 000€)
           ↓
02:20 UTC  Cache Redis invalidé pour les tags dim_customers
           ↓
02:25 UTC  Requêtes dashboard → Redis cache → réponse mesurée < 200ms (dev)
```

### Scénario 2 — Clickstream batch (Pub/Sub → GCS → Spark)

```
Dev        pubsub_clickstream_producer.py publie 10 000 events
           (~230 events/sec mesurés sur GCP Pub/Sub)
           ↓
           pubsub_spark_consumer.py consomme en batches de 1 000
           • acquitte après écriture Parquet (at-least-once garanti)
           • écrit vers gs://dataflow-dev-bronze/clickstream/ (10 fichiers)
           ↓
           clickstream_processor.py (PySpark Silver) :
           • parse timestamps (5 formats : ISO, Unix-ms, Unix-s…)
           • anonymise les IPs (RGPD)
           • déduplique sur event_id (first-write-wins)
           • classifie referrer_channel
           • normalise le revenu (€/$, virgule FR)
           • écrit Delta partitionné par event_date / event_type
           ↓
           daily_sales_aggregator.py (PySpark Gold) :
           • gold_daily_sales    (revenue par pays/canal)
           • gold_funnel_metrics (view → cart → checkout → purchase)
           • gold_product_performance (top produits)
           • gold_session_stats  (durée, bounce rate)
           • double write : BigQuery + Snowflake staging
```

### Scénario 3 — Autoscaling (design infrastructure)

L'infrastructure Terraform est conçue pour absorber les pics de charge
(Black Friday, soldes). Ce scénario décrit les capacités provisionnées,
non mesurées en production réelle.

```
Nominal     GKE : 3 nodes core (n2-standard-8)
            Spark pool : 0 nodes (scale-to-zero hors jobs)

Pic         HPA Airflow workers : 1 → 20 pods
            GKE Cluster Autoscaler : 3 → 20 nodes core
            Spark pool : 0 → 50 nodes (n2-highmem-16, 128 GB RAM)
            Pub/Sub absorbe les pics sans configuration manuelle

Après pic   Retour automatique à l'état nominal (économies de coût)
```

---

## 📂 Structure du repo

```
dataflow-ecommerce-platform/
│
├── ingestion/
│   ├── connectors/
│   │   ├── mongodb_extractor.py      # MongoDB → Snowflake (incrémental, RGPD, seed 50K)
│   │   ├── rest_api_connector.py     # Connecteur REST générique → GCS Bronze
│   │   └── pubsub_clickstream_producer.py  # Simulateur d'events Pub/Sub
│   ├── airbyte_connections/
│   │   ├── salesforce_crm.yaml       # Config Airbyte Salesforce → Snowflake
│   │   └── mongodb_user_profiles.yaml # Config Airbyte MongoDB → Snowflake
│   └── schemas/
│       └── clickstream_event_v2.json  # Contrat de schéma JSON
│
├── transforms/
│   ├── bronze/
│   │   └── raw_events_loader.py      # Micro-batch Kafka → GCS (pattern Bronze)
│   ├── silver/
│   │   ├── clickstream_processor.py  # PySpark : Bronze → Silver
│   │   └── pubsub_spark_consumer.py  # Pub/Sub consumer → Bronze Parquet
│   └── gold/
│       └── daily_sales_aggregator.py # PySpark : Silver → 4 tables Gold
│
├── models/                           # Projet dbt (star schema Snowflake)
│   ├── staging/
│   │   ├── stg_customers.sql         # Salesforce CRM, dédup, pseudonymisation
│   │   └── stg_orders.sql            # Multi-storefront FR/DE/ES normalisé
│   └── marts/
│       ├── dim_customers.sql          # SCD Type 2, churn_risk, is_high_value
│       ├── dim_products.sql           # SCD Type 2, gross_margin_rate
│       ├── dim_time.sql               # Spine 2020→2030, jours fériés FR
│       └── fact_orders.sql            # Fait central, attribution session
│
├── orchestration/
│   └── dags/
│       └── ecommerce_daily_pipeline.py  # DAG Airflow complet (14 tasks)
│
├── cache/
│   └── query_cache.py               # Cache Redis (msgpack+lz4, tags, décorateur)
│
├── scripts/
│   └── databricks_deploy.py         # Déploiement notebook Silver sur Databricks via API
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf                  # GKE, GCS, BigQuery, Redis, IAM, Secret Manager
│   │   └── variables.tf
│   └── k8s/
│       ├── airflow-deployment.yaml  # Webserver + Scheduler + Workers + HPA
│       ├── airflow-pvc.yaml         # PersistentVolumeClaim pour les DAGs
│       └── airflow-sa.yaml          # ServiceAccount + RBAC + Workload Identity
│
├── monitoring/
│   └── great_expectations/
│       └── clickstream_silver_checkpoint.py  # 15+ expectations (schéma, volume, fraîcheur)
│
├── tests/
│   ├── unit/
│   │   ├── test_clickstream_processor.py  # Tests PySpark (SparkSession locale)
│   │   └── test_query_cache.py            # Tests Redis (fakeredis, pas de Redis réel)
│   └── integration/
│       └── test_pipeline_end_to_end.py    # Pipeline complet Bronze→Silver + cache
│
├── .github/
│   └── workflows/
│       └── ci.yml                   # CI : lint → pytest → dbt → deploy
│
├── .env.example
├── requirements.txt                 # Dépendances dev local
├── requirements-cloud.txt           # Dépendances GKE workers
└── conftest.py                      # PYTHONPATH pytest
```

---

## 🔑 Prérequis

### Comptes cloud (tous gratuits)

| Service | Plan | Coût | Lien |
|---------|------|------|------|
| **Snowflake** | Trial (30 jours, $400 crédits) | Gratuit | [trial.snowflake.com](https://trial.snowflake.com) |
| **MongoDB Atlas** | M0 Free cluster | Gratuit | [cloud.mongodb.com](https://cloud.mongodb.com) |
| **GCP** | Free tier + billing activé | ~$0.05/jour | [console.cloud.google.com](https://console.cloud.google.com) |
| **Azure Databricks** | Trial ou workspace existant | Gratuit | [azure.microsoft.com](https://azure.microsoft.com) |

> **Note :** Le mode `STORAGE_MODE=local` permet de tester sans GCP (pas de billing requis).

### Outils locaux

```bash
python --version   # 3.11+
java -version      # 11 ou 17 (requis pour PySpark local)
docker --version   # 20+
gcloud --version   # Google Cloud SDK
git --version      # 2+
```

---

## 🚀 Installation

### Étape 1 — Clone & environnement Python

```bash
git clone https://github.com/JEMALIACHRAF/dataflow-ecommerce-platform.git
cd dataflow-ecommerce-platform

python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\Activate.ps1      # Windows PowerShell

pip install -r requirements.txt
```

### Étape 2 — Configurer Snowflake

```sql
-- Dans un Snowflake Worksheet (rôle ACCOUNTADMIN)
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
```

### Étape 3 — Configurer MongoDB Atlas

1. [cloud.mongodb.com](https://cloud.mongodb.com) → Cluster M0 Free → AWS Frankfurt
2. Database Access → Créer user `dataflow_user`
3. Network Access → Allow `0.0.0.0/0`
4. Copier la connection string

### Étape 4 — Configurer GCP (optionnel)

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

gcloud services enable pubsub.googleapis.com storage.googleapis.com bigquery.googleapis.com

# Créer les buckets Medallion
gcloud storage buckets create gs://dataflow-dev-bronze --location=EU --uniform-bucket-level-access
gcloud storage buckets create gs://dataflow-dev-silver --location=EU --uniform-bucket-level-access
gcloud storage buckets create gs://dataflow-dev-gold   --location=EU --uniform-bucket-level-access

# Créer le topic Pub/Sub
gcloud pubsub topics create clickstream-events
gcloud pubsub subscriptions create clickstream-events-sub --topic=clickstream-events
```

> Si GCP n'est pas configuré, mettre `STORAGE_MODE=local` dans `.env` — le pipeline fonctionne intégralement en local.

### Étape 5 — Fichier `.env`

```bash
cp .env.example .env
# Remplir avec vos credentials
```

```ini
# Snowflake
SNOWFLAKE_ACCOUNT=XXXXXXX.eu-west-3.aws
SNOWFLAKE_USER=YOUR_USER
SNOWFLAKE_PASSWORD=YOUR_PASSWORD
SNOWFLAKE_DATABASE=DATAFLOW_DEV
SNOWFLAKE_WAREHOUSE=ANALYTICS_WH
SNOWFLAKE_ROLE=TRANSFORMER_ROLE

# MongoDB Atlas
MONGODB_URI=mongodb+srv://dataflow_user:PASSWORD@cluster0.xxxxx.mongodb.net/users

# GCP (optionnel si STORAGE_MODE=local)
GCP_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-key.json
GCS_BRONZE_BUCKET=gs://dataflow-dev-bronze
GCS_SILVER_BUCKET=gs://dataflow-dev-silver
GCS_GOLD_BUCKET=gs://dataflow-dev-gold
STORAGE_MODE=auto   # ou local

# Redis
REDIS_URL=redis://localhost:6379/0

# Databricks (optionnel)
DATABRICKS_TOKEN=your-pat-token
```

### Étape 6 — Configurer dbt

```bash
cd models/dataflow_ecommerce
dbt init dataflow_ecommerce   # suivre les prompts Snowflake
dbt debug                     # vérifier la connexion
```

---

## ▶️ Exécution

### 1. MongoDB → Snowflake (profils utilisateurs)

```bash
# Seed automatique 50K profils + extraction vers Snowflake
python ingestion/connectors/mongodb_extractor.py

# Vérifier dans Snowflake :
# SELECT COUNT(*) FROM DATAFLOW_DEV.RAW_MONGODB.STG_MONGO_PROFILES;
```

### 2. dbt — star schema Snowflake

```bash
cd models/dataflow_ecommerce
dbt run    # construit tous les modèles (~10 sec)
dbt test   # 32 tests de qualité (~14 sec)
dbt docs generate && dbt docs serve  # DAG interactif sur localhost:8080
```

### 3. Clickstream (Pub/Sub → GCS → Spark)

```bash
# Terminal 1 : publier des events vers Pub/Sub
python ingestion/connectors/pubsub_clickstream_producer.py --n-events 10000

# Terminal 2 : consommer et écrire en Bronze Parquet
python transforms/silver/pubsub_spark_consumer.py \
  --bronze-path gs://dataflow-dev-bronze/clickstream \
  --skip-spark   # pour envoyer sur Databricks plutôt que local

# Déployer et lancer le job Silver sur Databricks
python scripts/databricks_deploy.py
```

### 4. Cache Redis

```bash
# Démarrer Redis
docker run -d --name redis-dataflow -p 6380:6379 \
  redis:7.2-alpine redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

# Tester le cache
python -c "from cache.query_cache import QueryCache, CacheTTL; \
  c = QueryCache.from_url('redis://localhost:6380'); \
  print(c.stats())"
```

### 5. Airflow DAG

```bash
docker cp orchestration/dags/ecommerce_daily_pipeline.py \
  <airflow-container>:/opt/airflow/dags/

docker exec <airflow-container> airflow dags trigger ecommerce_daily_pipeline
```

### 6. Tests

```bash
# Tous les tests (aucun cloud requis)
pytest tests/unit/ -v --cov=transforms --cov=cache --cov-report=term-missing

# Tests d'intégration (Spark local + fakeredis)
pytest tests/integration/ -v -m "not slow"

# Smoke test 10M events (plus lent)
pytest tests/integration/ -v -m "slow"
```

### 7. Infrastructure GKE (optionnel — nécessite billing GCP)

```bash
gcloud container clusters create dataflow-dev \
  --zone=europe-west9-a \
  --num-nodes=1 \
  --machine-type=e2-standard-2

gcloud container clusters get-credentials dataflow-dev --zone=europe-west9-a
kubectl apply -f infrastructure/k8s/airflow-sa.yaml
kubectl apply -f infrastructure/k8s/airflow-pvc.yaml
kubectl apply -f infrastructure/k8s/airflow-deployment.yaml

# ⚠️ Supprimer le cluster après les tests pour éviter les coûts
gcloud container clusters delete dataflow-dev --zone=europe-west9-a --quiet
```

---

## 📊 Benchmarks mesurés

Ces métriques sont mesurées sur l'environnement de développement (comptes gratuits, ressources limitées).
Elles illustrent la faisabilité des patterns, pas les performances de production.

| Opération | Mesure | Environnement |
|-----------|--------|---------------|
| MongoDB seed (50K profils) | ~13 min | Atlas M0 free, latence réseau |
| Extraction MongoDB (50K docs) | ~6 min | Incremental, transform RGPD |
| dbt full run (4 modèles) | ~10 sec | Snowflake X-Small |
| dbt test suite (32 tests) | ~14 sec | Parallèle |
| Pub/Sub publish (10K events) | ~44 sec | ~227 events/sec |
| Pub/Sub consume → Parquet (10K) | ~30 sec | 10 fichiers Parquet |
| Redis SET/GET | < 1ms | msgpack + lz4, local |
| Redis hit rate (tests) | ~68% | allkeys-lru, requêtes répétées |
| Snowflake query (50K rows) | < 600ms | X-Small warehouse |
| Spark 10M events (smoke test) | < 60s | local[2], 2 workers |

---

## 🏗️ Infrastructure

### Ressources GCP (dev)

| Ressource | Type | Usage |
|-----------|------|-------|
| `dataflow-dev-bronze` | GCS Bucket (EU) | Parquet brut — append only |
| `dataflow-dev-silver` | GCS Bucket (EU) | Delta nettoyé |
| `dataflow-dev-gold` | GCS Bucket (EU) | Agrégations métier |
| `clickstream-events` | Pub/Sub Topic | Flux d'events clickstream |
| `clickstream-events-sub` | Pub/Sub Subscription | Consumer Spark |
| `dataflow-dev` | GKE Cluster (europe-west9-a) | Orchestration Airflow |
| `dataflow-dev-sa` | Service Account | IAM least-privilege |

### Ressources Snowflake

| Ressource | Type |
|-----------|------|
| `DATAFLOW_DEV` | Database |
| `RAW_MONGODB`, `RAW_SALESFORCE`, `SILVER`, `MARTS`, `INTERNAL` | Schemas |
| `INGEST_WH`, `ANALYTICS_WH` | Virtual Warehouses (X-Small) |
| `TRANSFORMER_ROLE` | Rôle least-privilege |

### Infrastructure Terraform (prod design)

Le Terraform provisionne un environnement production complet avec :
- GKE : node pool core (n2-standard-8, 3→20 nodes) + node pool Spark (n2-highmem-16, 0→50 nodes, scale-to-zero)
- Redis Memorystore : 32 GB HA en prod, 4 GB Basic en staging
- Lifecycle rules GCS : Bronze → Nearline après 90j → Coldline après 1 an
- Workload Identity sur GKE (pas de fichiers de clé dans les pods)
- Secret Manager pour les credentials Snowflake, MongoDB, Redis

---

## 🔄 CI/CD

Pipeline déclenché sur chaque push vers `main` ou `develop` :

```
push → main
       │
       ▼
[Unit Tests]         pytest tests/unit/ --cov (SparkSession locale, fakeredis)
       │ PASS
       ▼
[dbt Tests]          dbt compile + dbt test sur schema CI Snowflake
       │ PASS
       ▼
[Validate Configs]   Validation YAML + JSON schemas
       │ PASS
       ▼
[Deploy]             dbt run --select marts.* (main uniquement)
       │
       ▼
[Notify Slack]       ✅ succès / 🔴 échec
```

Secrets stockés dans GitHub Actions (jamais dans le code) :
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`, `GCP_SA_KEY`

---

## 👤 Author

**Achraf Jemali** — Data & AI Engineer. 
Paris, France 🇫🇷  
[![GitHub](https://img.shields.io/badge/GitHub-JEMALIACHRAF-black?logo=github&style=flat-square)](https://github.com/JEMALIACHRAF)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Achraf_Jemali-0077B5?logo=linkedin&style=flat-square)](https://linkedin.com/in/achraf-jemali-54a417239)



---
