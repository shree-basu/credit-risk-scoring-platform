output "deployment_requested" {
  description = "True only after both explicit core deployment gates are open."
  value       = local.deployment_requested
}

output "composer_requested" {
  description = "True only after the separate Composer gates are also open."
  value       = local.composer_requested
}

output "raw_bucket" {
  value = try(google_storage_bucket.data["raw"].name, null)
}

output "artifact_bucket" {
  value = try(google_storage_bucket.data["artifacts"].name, null)
}

output "spark_service_account" {
  value = try(google_service_account.workload["spark"].email, null)
}

output "vertex_service_account" {
  value = try(google_service_account.workload["vertex"].email, null)
}
