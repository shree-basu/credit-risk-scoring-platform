resource "google_dataproc_cluster" "feature_cluster" {
name   = "credit-risk-feature-cluster"
region = var.region
cluster_config {


master_config {
  num_instances = 1
  machine_type  = "n1-standard-2"
}

worker_config {
  num_instances = 2
  machine_type  = "n1-standard-2"
}

software_config {
  image_version = "2.1-debian11"
}

}
}


