# infrastructure/terraform/variables.tf

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region"
  type        = string
  default     = "europe-west1"   # Belgium — lowest latency from Paris
}

variable "gcs_location" {
  description = "GCS multi-region location (EU covers all European regions)"
  type        = string
  default     = "EU"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU"
}

variable "core_node_count" {
  description = "Initial node count for the core GKE node pool"
  type        = number
  default     = 3
}
