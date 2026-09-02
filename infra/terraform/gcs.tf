resource "google_storage_bucket" "data" {
  for_each = local.deployment_requested ? {
    raw       = local.raw_bucket_name
    artifacts = local.artifact_bucket_name
  } : {}

  name                        = each.value
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.common_labels

  versioning {
    enabled = true
  }

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.core]
}
