variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Região GCP do cluster"
}

variable "cluster_name" {
  type        = string
  default     = "spark-alfabetizacao"
  description = "Nome do cluster Dataproc"
}

variable "machine_type" {
  type        = string
  default     = "n1-standard-4"
  description = "Tipo de máquina (4 vCPU, 15GB RAM)"
}

variable "num_workers" {
  type        = number
  default     = 2
  description = "Número de worker nodes"
}

