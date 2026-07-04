variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "bucket_name" {
  type        = string
  description = "Nome do bucket GCS (deve ser globalmente único no GCP)"
}

variable "location" {
  type        = string
  default     = "US"
  description = "Localização do bucket (US = multi-region, mais barato)"
}
