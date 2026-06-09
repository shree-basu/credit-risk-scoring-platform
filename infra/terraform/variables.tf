variable "project_id" {
  description = "The GCP project ID where resources are created"
  type        = string
}

variable "region" {
  description = "The GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "raw_bucket_name" {
  description = "Name of the GCS bucket for raw loan data"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset for feature and scored tables"
  type        = string
  default     = "credit_risk"
}