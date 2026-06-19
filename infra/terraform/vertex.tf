resource "google_project_service" 
"vertex_ai" { 
    service = "aiplatform.googleapis.com" 
    disable_on_destroy = false } 
    
resource "google_storage_bucket" 
"vertex_staging" { 
    name = 
    "${var.project_id}-vertex-staging" 
    location = var.region 
    force_destroy = true 
    uniform_bucket_level_access = true 
    } 