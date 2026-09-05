locals {
  core_services = toset([
    "aiplatform.googleapis.com",
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_project_service" "core" {
  for_each = local.deployment_requested ? local.core_services : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_project_service" "composer" {
  for_each = local.composer_requested ? toset(["composer.googleapis.com"]) : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
