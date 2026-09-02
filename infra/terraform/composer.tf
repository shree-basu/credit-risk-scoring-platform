resource "google_composer_environment" "orchestration" {
  for_each = local.composer

  project = var.project_id
  name    = "credit-risk-orchestration"
  region  = var.region
  labels  = local.common_labels

  config {
    software_config {
      image_version = var.composer_image_version

      env_variables = {
        AIRFLOW_VAR_GCP_PROJECT_ID         = var.project_id
        AIRFLOW_VAR_GCP_REGION             = var.region
        AIRFLOW_VAR_RAW_BUCKET             = google_storage_bucket.data["raw"].name
        AIRFLOW_VAR_ARTIFACT_BUCKET        = google_storage_bucket.data["artifacts"].name
        AIRFLOW_VAR_SPARK_SERVICE_ACCOUNT  = google_service_account.workload["spark"].email
        AIRFLOW_VAR_VERTEX_SERVICE_ACCOUNT = google_service_account.workload["vertex"].email
        AIRFLOW_VAR_VERTEX_TRAINER_IMAGE   = "${var.region}-docker.pkg.dev/${var.project_id}/credit-risk/trainer:approved"
        AIRFLOW_VAR_VERTEX_PREDICTOR_IMAGE = "${var.region}-docker.pkg.dev/${var.project_id}/credit-risk/predictor:approved"
        AIRFLOW_VAR_VERTEX_PARENT_MODEL    = var.vertex_parent_model
      }
    }

    node_config {
      service_account = google_service_account.composer["active"].email
    }
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_iam_member.composer,
    google_service_account_iam_member.composer_service_agent_extension,
    google_service_account_iam_member.composer_act_as_workload,
  ]
}
