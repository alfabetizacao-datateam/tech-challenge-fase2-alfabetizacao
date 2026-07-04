variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "bucket_name" {
  type        = string
  description = "Bucket GCS que contém os Parquet Gold"
}

variable "dataset_id" {
  type        = string
  default     = "gold"
  description = "ID do dataset BigQuery"
}

variable "location" {
  type        = string
  default     = "US"
  description = "Localização do dataset (deve ser mesma do bucket GCS)"
}

variable "create_tables" {
  type        = bool
  default     = false
  description = "Criar external tables? false=Fase1 (sem dados), true=Fase2 (após Spark)"
}
