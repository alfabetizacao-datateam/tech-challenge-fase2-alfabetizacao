terraform {
  required_version = ">= 1.7.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Obtém número do projeto para construir o email da service account
data "google_project" "project" {
  project_id = var.project_id
}

# Concede permissão Storage ao Compute Engine default SA (necessário para Dataproc)
resource "google_project_iam_member" "compute_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

module "gcs" {
  source = "./modules/gcs"

  project_id  = var.project_id
  bucket_name = var.bucket_name
  location    = "US"
}

module "dataproc" {
  source = "./modules/dataproc"

  project_id   = var.project_id
  region       = var.region
  cluster_name = var.cluster_name
  num_workers  = var.num_workers

  depends_on = [module.gcs, google_project_iam_member.compute_storage_admin]
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id    = var.project_id
  bucket_name   = module.gcs.bucket_name
  location      = "US"
  create_tables = var.create_bq_tables

  depends_on = [module.gcs]
}
