variable "project_id" {
  description = "Target GCP project. The safe default is intentionally unusable."
  type        = string
  default     = "replace-me"
}

variable "region" {
  description = "Regional location for BigQuery, Managed Spark, Vertex AI and Composer."
  type        = string
  default     = "us-central1"
}

variable "project_number" {
  description = "Numeric GCP project number; required only when Composer is explicitly enabled."
  type        = string
  default     = null
  nullable    = true
}

variable "deployment_enabled" {
  description = "First deliberate deployment gate. False produces no managed resources."
  type        = bool
  default     = false
}

variable "deployment_confirmation" {
  description = "Second deployment gate. Must equal DEPLOY together with deployment_enabled."
  type        = string
  default     = ""
}

variable "enable_composer" {
  description = "Separate opt-in for the relatively expensive Composer environment."
  type        = bool
  default     = false
}

variable "composer_confirmation" {
  description = "Must equal COMPOSER in addition to both deployment gates."
  type        = string
  default     = ""
}

variable "composer_image_version" {
  description = "Composer 3 image alias with the Airflow version tested by the project."
  type        = string
  default     = "composer-3-airflow-2.11.1"
}

variable "vertex_parent_model" {
  description = "Existing fully qualified Vertex model resource required by periodic retraining."
  type        = string
  default     = ""
}

variable "raw_bucket_name" {
  description = "Optional globally unique raw bucket name."
  type        = string
  default     = null
  nullable    = true
}

variable "artifact_bucket_name" {
  description = "Optional globally unique code, model and evidence bucket name."
  type        = string
  default     = null
  nullable    = true
}

variable "labels" {
  description = "Additional labels applied to supported resources."
  type        = map(string)
  default     = {}
}
