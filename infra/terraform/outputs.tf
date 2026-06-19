output "raw_bucket" { 
    description = "Name of the GCS raw data bucket" 
    value = google_storage_bucket.raw_data.name 
    } 
    
output "dataset" { 
    description = "Name of the dataset in bigquery" 
    value = google_bigquery_dataset.credit_risk.dataset_id 
    } 
    
output "dataproc_cluster" { 
    description = "Name of the dataproc cluster on GCP" 
    value = google_dataproc_cluster.feature_cluster.name 
}