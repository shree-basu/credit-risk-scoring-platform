locals {
  workload_project_roles = local.deployment_requested ? {
    spark_bigquery_jobs  = ["spark", "roles/bigquery.jobUser"]
    spark_dataproc       = ["spark", "roles/dataproc.worker"]
    spark_logging        = ["spark", "roles/logging.logWriter"]
    vertex_ai            = ["vertex", "roles/aiplatform.user"]
    vertex_bigquery_jobs = ["vertex", "roles/bigquery.jobUser"]
    vertex_logging       = ["vertex", "roles/logging.logWriter"]
  } : {}

  composer_project_roles = local.composer_requested ? {
    composer_worker   = "roles/composer.worker"
    composer_bq_jobs  = "roles/bigquery.jobUser"
    composer_dataproc = "roles/dataproc.editor"
    composer_vertex   = "roles/aiplatform.user"
    composer_logging  = "roles/logging.logWriter"
  } : {}
}

resource "google_project_iam_member" "workload" {
  for_each = local.workload_project_roles

  project = var.project_id
  role    = each.value[1]
  member  = "serviceAccount:${google_service_account.workload[each.value[0]].email}"
}

resource "google_project_iam_member" "composer" {
  for_each = local.composer_project_roles

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer["active"].email}"
}

resource "google_service_account_iam_member" "composer_service_agent_extension" {
  for_each = local.composer

  service_account_id = google_service_account.composer["active"].name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:service-${coalesce(var.project_number, "000000000000")}@cloudcomposer-accounts.iam.gserviceaccount.com"

  lifecycle {
    precondition {
      condition     = var.project_number != null && can(regex("^[0-9]+$", var.project_number))
      error_message = "A numeric project_number is required before Composer can be enabled."
    }
  }
}

resource "google_storage_bucket_iam_member" "spark_raw_reader" {
  for_each = local.deployment

  bucket = google_storage_bucket.data["raw"].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workload["spark"].email}"
}

resource "google_storage_bucket_iam_member" "spark_artifact_reader" {
  for_each = local.deployment

  bucket = google_storage_bucket.data["artifacts"].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workload["spark"].email}"
}

resource "google_storage_bucket_iam_member" "vertex_artifact_writer" {
  for_each = local.deployment

  bucket = google_storage_bucket.data["artifacts"].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workload["vertex"].email}"
}

resource "google_bigquery_dataset_iam_member" "spark_dataset" {
  for_each = local.deployment_requested ? toset(["features", "audit"]) : toset([])

  project    = var.project_id
  dataset_id = google_bigquery_dataset.warehouse[each.value].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.workload["spark"].email}"
}

resource "google_bigquery_dataset_iam_member" "vertex_feature_reader" {
  for_each = local.deployment

  project    = var.project_id
  dataset_id = google_bigquery_dataset.warehouse["features"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.workload["vertex"].email}"
}

resource "google_bigquery_dataset_iam_member" "vertex_writer" {
  for_each = local.deployment_requested ? toset(["scoring", "audit", "staging"]) : toset([])

  project    = var.project_id
  dataset_id = google_bigquery_dataset.warehouse[each.value].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.workload["vertex"].email}"
}

resource "google_artifact_registry_repository_iam_member" "vertex_image_reader" {
  for_each = local.deployment

  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.containers["active"].name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.workload["vertex"].email}"
}

resource "google_service_account_iam_member" "composer_act_as_workload" {
  for_each = local.composer_requested ? toset(["spark", "vertex"]) : toset([])

  service_account_id = google_service_account.workload[each.value].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer["active"].email}"
}

resource "google_storage_bucket_iam_member" "composer_bucket_access" {
  for_each = local.composer_requested ? toset(["raw", "artifacts"]) : toset([])

  bucket = google_storage_bucket.data[each.value].name
  role   = each.value == "raw" ? "roles/storage.objectViewer" : "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.composer["active"].email}"
}

resource "google_bigquery_dataset_iam_member" "composer_dataset" {
  for_each = local.composer_requested ? toset(["features", "scoring", "audit", "staging"]) : toset([])

  project    = var.project_id
  dataset_id = google_bigquery_dataset.warehouse[each.value].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.composer["active"].email}"
}
