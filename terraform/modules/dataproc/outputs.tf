output "cluster_name" {
  value       = google_dataproc_cluster.spark_cluster.name
  description = "Nome do cluster para uso no gcloud dataproc jobs submit"
}

output "cluster_region" {
  value       = google_dataproc_cluster.spark_cluster.region
  description = "Região do cluster"
}
