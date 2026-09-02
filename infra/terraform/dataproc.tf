resource "google_service_account" "workload" {
  for_each = local.deployment_requested ? toset(["spark", "vertex"]) : toset([])

  project      = var.project_id
  account_id   = "credit-risk-${each.key}"
  display_name = "Credit risk ${each.key} workload"

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.core]
}

resource "google_service_account" "composer" {
  for_each = local.composer

  project      = var.project_id
  account_id   = "credit-risk-composer"
  display_name = "Credit risk Composer orchestration"

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.composer]
}


