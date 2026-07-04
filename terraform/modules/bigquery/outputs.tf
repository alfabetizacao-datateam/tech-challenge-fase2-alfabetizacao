output "dataset_id" {
  value       = google_bigquery_dataset.gold.dataset_id
  description = "ID do dataset BigQuery para uso em queries"
}

output "dataset_project" {
  value       = google_bigquery_dataset.gold.project
  description = "Projeto onde o dataset reside"
}
