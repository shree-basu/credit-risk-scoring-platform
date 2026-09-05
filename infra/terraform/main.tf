provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  deployment_requested = var.deployment_enabled && var.deployment_confirmation == "DEPLOY"
  composer_requested = (
    local.deployment_requested &&
    var.enable_composer &&
    var.composer_confirmation == "COMPOSER"
  )

  deployment = local.deployment_requested ? { active = true } : {}
  composer   = local.composer_requested ? { active = true } : {}

  raw_bucket_name      = coalesce(var.raw_bucket_name, "${var.project_id}-credit-risk-raw")
  artifact_bucket_name = coalesce(var.artifact_bucket_name, "${var.project_id}-credit-risk-artifacts")
  common_labels        = merge({ project = "credit-risk", managed-by = "terraform" }, var.labels)
}
