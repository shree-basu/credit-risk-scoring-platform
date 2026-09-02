locals {
  datasets = {
    features = { id = "credit_risk_features", expiry = null }
    scoring  = { id = "credit_risk_scoring", expiry = null }
    audit    = { id = "credit_risk_audit", expiry = null }
    staging  = { id = "credit_risk_staging", expiry = 604800000 }
  }

  tables = {
    training_features = {
      dataset = "features", partition = "feature_date", cluster = ["loan_id", "feature_version"]
    }
    scoring_features = {
      dataset = "features", partition = "feature_date", cluster = ["loan_id", "feature_version"]
    }
    risk_scores = {
      dataset = "scoring", partition = "score_date", cluster = ["model_version", "risk_band", "loan_id"]
    }
    pipeline_runs = {
      dataset = "audit", partition = "logical_date", cluster = ["dag_id", "status"]
    }
    dq_results = {
      dataset = "audit", partition = "business_date", cluster = ["run_id", "status"]
    }
    quarantine_records = {
      dataset = "audit", partition = "business_date", cluster = ["batch_id", "reason_code"]
    }
    training_runs = {
      dataset = "audit", partition = "training_snapshot", cluster = ["status", "model_version"]
    }
    model_assignments = {
      dataset = "audit", partition = "effective_from", cluster = ["status", "model_version"]
    }
    drift_metrics = {
      dataset = "audit", partition = "current_period", cluster = ["status", "feature_name", "metric"]
    }
  }
}

resource "google_bigquery_dataset" "warehouse" {
  for_each = local.deployment_requested ? local.datasets : {}

  project                     = var.project_id
  dataset_id                  = each.value.id
  location                    = var.region
  description                 = "Credit-risk ${each.key} data"
  delete_contents_on_destroy  = false
  default_table_expiration_ms = each.value.expiry
  labels                      = local.common_labels

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.core]
}

resource "google_bigquery_table" "warehouse" {
  for_each = local.deployment_requested ? local.tables : {}

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.warehouse[each.value.dataset].dataset_id
  table_id            = each.key
  description         = "Governed credit-risk ${replace(each.key, "_", " ")} table"
  schema              = file("${path.module}/schemas/${each.key}.json")
  deletion_protection = true
  clustering          = each.value.cluster

  time_partitioning {
    type                     = "DAY"
    field                    = each.value.partition
    require_partition_filter = true
  }

  lifecycle {
    prevent_destroy = true
  }
}
