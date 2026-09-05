resource "google_artifact_registry_repository" "containers" {
  for_each = local.deployment

  project         = var.project_id
  location        = var.region
  repository_id   = "credit-risk"
  description     = "Trainer and prediction images for the credit-risk reference platform"
  format          = "DOCKER"
  mode            = "STANDARD_REPOSITORY"
  deletion_policy = "PREVENT"
  labels          = local.common_labels

  docker_config {
    immutable_tags = true
  }

  cleanup_policy_dry_run = true

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.core]
}
