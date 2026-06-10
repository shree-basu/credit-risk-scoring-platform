resource "google_storage_bucket" "raw_data" {
  name          = var.raw_bucket_name 
  location      = var.region

  
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}
