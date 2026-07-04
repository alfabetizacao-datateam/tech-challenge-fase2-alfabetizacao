output "bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "Nome do bucket GCS"
}

output "bucket_url" {
  value       = "gs://${google_storage_bucket.data_lake.name}"
  description = "URL GCS para uso nos scripts Spark"
}
