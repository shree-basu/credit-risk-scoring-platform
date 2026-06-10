resource "google_bigquery_dataset" "credit_risk" {
  dataset_id  = var.dataset_id
  location    = var.region
  description = "Feature and scored tables for the credit risk model"

  delete_contents_on_destroy = true
}
