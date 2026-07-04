variable "project_id" {
  type        = string
  description = "GCP Project ID com billing habilitado"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Região GCP (us-central1 = mais barata)"
}

variable "bucket_name" {
  type        = string
  description = "Nome único global do bucket GCS"
}

variable "cluster_name" {
  type        = string
  default     = "spark-alfabetizacao"
  description = "Nome do cluster Dataproc"
}

variable "num_workers" {
  type        = number
  default     = 2
  description = "Número de workers (2 = FinOps balanceado)"
}

variable "create_bq_tables" {
  type        = bool
  default     = false
  description = "false=Fase1 (só cria infra), true=Fase2 (após Spark gerar dados Gold)"
}
