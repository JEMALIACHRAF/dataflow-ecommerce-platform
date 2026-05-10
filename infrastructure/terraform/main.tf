# infrastructure/terraform/main.tf
#
# DataFlow E-Commerce Platform — GCP Infrastructure
# Provisions: GCS buckets (Medallion), BigQuery datasets,
#             GKE cluster (Airflow + Spark), Redis (Memorystore),
#             Secret Manager entries, IAM service accounts.
#
# Usage:
#   terraform init
#   terraform workspace select prod   # or staging
#   terraform plan -var-file=envs/prod.tfvars
#   terraform apply -var-file=envs/prod.tfvars

terraform {
  required_version = ">= 1.7"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.25"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.25"
    }
  }

  # Remote state in GCS — one bucket per workspace (prod / staging)
  backend "gcs" {
    bucket = "dataflow-tf-state"
    prefix = "ecommerce-platform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  env    = terraform.workspace   # "prod" or "staging"
  labels = {
    team        = "data-engineering"
    project     = "ecommerce-platform"
    environment = local.env
    managed_by  = "terraform"
  }
}

# =============================================================================
# GCS — Medallion Lakehouse Buckets
# =============================================================================

resource "google_storage_bucket" "bronze" {
  name                        = "dataflow-${local.env}-bronze"
  location                    = var.gcs_location   # "EU" for multi-region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  labels                      = local.labels

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
  lifecycle_rule {
    condition { age = 365 }
    action    { type = "SetStorageClass"; storage_class = "COLDLINE" }
  }

  versioning { enabled = false }   # Bronze is append-only; no versioning needed

  cors {
    origin          = ["https://console.cloud.google.com"]
    method          = ["GET"]
    response_header = ["Content-Type"]
    max_age_seconds = 3600
  }
}

resource "google_storage_bucket" "silver" {
  name                        = "dataflow-${local.env}-silver"
  location                    = var.gcs_location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  labels                      = local.labels

  lifecycle_rule {
    condition { age = 180 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
}

resource "google_storage_bucket" "gold" {
  name                        = "dataflow-${local.env}-gold"
  location                    = var.gcs_location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  labels                      = local.labels
  # Gold never goes to cold storage — always needed for dashboards
}

resource "google_storage_bucket" "scripts" {
  name                        = "dataflow-${local.env}-scripts"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  labels                      = local.labels
}

# =============================================================================
# BigQuery — Gold Layer Datasets
# =============================================================================

resource "google_bigquery_dataset" "gold_layer" {
  dataset_id                  = "gold_layer_${local.env}"
  friendly_name               = "Gold Layer — ${upper(local.env)}"
  description                 = "Business-ready aggregations (daily sales, funnel, product perf)"
  location                    = var.bq_location
  delete_contents_on_destroy  = local.env == "staging"
  labels                      = local.labels

  access {
    role          = "OWNER"
    user_by_email = google_service_account.spark_runner.email
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_dataset" "silver_layer" {
  dataset_id   = "silver_layer_${local.env}"
  friendly_name = "Silver Layer — ${upper(local.env)}"
  description  = "Cleaned and typed event data from Spark jobs"
  location     = var.bq_location
  labels       = local.labels
}

# =============================================================================
# GKE — Airflow + Spark Operator Cluster
# =============================================================================

resource "google_container_cluster" "dataflow" {
  name                     = "dataflow-${local.env}"
  location                 = var.region
  remove_default_node_pool = true
  initial_node_count       = 1
  labels                   = local.labels

  # Workload Identity — pods authenticate as service accounts (no key files)
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  network_policy {
    enabled = true
  }

  addons_config {
    horizontal_pod_autoscaling { disabled = false }
    http_load_balancing        { disabled = false }
  }

  maintenance_policy {
    daily_maintenance_window { start_time = "02:00" }  # 02:00 UTC — off-peak
  }

  resource_labels = local.labels
}

# Core node pool — Airflow workers + general workloads
resource "google_container_node_pool" "core" {
  name       = "core"
  cluster    = google_container_cluster.dataflow.name
  location   = var.region
  node_count = var.core_node_count

  autoscaling {
    min_node_count = 3
    max_node_count = 20   # auto-scales on Black Friday / soldes
  }

  node_config {
    machine_type    = "n2-standard-8"    # 8 vCPU, 32 GB RAM
    disk_size_gb    = 100
    disk_type       = "pd-ssd"
    service_account = google_service_account.gke_node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = local.labels

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Spark node pool — large memory for PySpark jobs
resource "google_container_node_pool" "spark" {
  name     = "spark"
  cluster  = google_container_cluster.dataflow.name
  location = var.region

  autoscaling {
    min_node_count = 0    # scale to zero when no jobs running
    max_node_count = 50
  }

  node_config {
    machine_type    = "n2-highmem-16"   # 16 vCPU, 128 GB RAM — Spark executors
    disk_size_gb    = 200
    disk_type       = "pd-ssd"
    service_account = google_service_account.gke_node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    taint {
      key    = "dedicated"
      value  = "spark"
      effect = "NO_SCHEDULE"
    }

    labels = merge(local.labels, { pool = "spark" })
  }
}

# =============================================================================
# Redis — Memorystore (managed Redis)
# =============================================================================

resource "google_redis_instance" "query_cache" {
  provider           = google-beta
  name               = "dataflow-${local.env}-cache"
  tier               = local.env == "prod" ? "STANDARD_HA" : "BASIC"
  memory_size_gb     = local.env == "prod" ? 32 : 4
  region             = var.region
  redis_version      = "REDIS_7_2"
  display_name       = "Query Cache — ${upper(local.env)}"
  auth_enabled       = true
  transit_encryption_mode = "SERVER_AUTHENTICATION"

  redis_configs = {
    "maxmemory-policy"    = "allkeys-lru"
    "activedefrag"        = "yes"
    "lazyfree-lazy-eviction" = "yes"
  }

  labels = local.labels
}

# =============================================================================
# Service Accounts
# =============================================================================

resource "google_service_account" "spark_runner" {
  account_id   = "spark-runner-${local.env}"
  display_name = "Spark Runner — ${upper(local.env)}"
}

resource "google_service_account" "airflow_worker" {
  account_id   = "airflow-worker-${local.env}"
  display_name = "Airflow Worker — ${upper(local.env)}"
}

resource "google_service_account" "gke_node" {
  account_id   = "gke-node-${local.env}"
  display_name = "GKE Node SA — ${upper(local.env)}"
}

# IAM bindings — least privilege
resource "google_project_iam_member" "spark_gcs_rw" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.spark_runner.email}"
}

resource "google_project_iam_member" "spark_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.spark_runner.email}"
}

resource "google_project_iam_member" "airflow_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.airflow_worker.email}"
}

resource "google_project_iam_member" "airflow_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.airflow_worker.email}"
}

# =============================================================================
# Secret Manager — credentials (Snowflake, MongoDB, Airbyte)
# =============================================================================

resource "google_secret_manager_secret" "snowflake_password" {
  secret_id = "snowflake-password-${local.env}"
  labels    = local.labels
  replication { auto {} }
}

resource "google_secret_manager_secret" "mongodb_uri" {
  secret_id = "mongodb-uri-${local.env}"
  labels    = local.labels
  replication { auto {} }
}

resource "google_secret_manager_secret" "airbyte_api_key" {
  secret_id = "airbyte-api-key-${local.env}"
  labels    = local.labels
  replication { auto {} }
}

resource "google_secret_manager_secret" "redis_auth_string" {
  secret_id = "redis-auth-${local.env}"
  labels    = local.labels
  replication { auto {} }
}

# =============================================================================
# Outputs
# =============================================================================

output "gke_cluster_name" {
  value = google_container_cluster.dataflow.name
}

output "bronze_bucket" {
  value = google_storage_bucket.bronze.name
}

output "silver_bucket" {
  value = google_storage_bucket.silver.name
}

output "gold_bucket" {
  value = google_storage_bucket.gold.name
}

output "redis_host" {
  value     = google_redis_instance.query_cache.host
  sensitive = true
}

output "redis_port" {
  value = google_redis_instance.query_cache.port
}
