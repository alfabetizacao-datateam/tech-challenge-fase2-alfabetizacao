resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.location
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true
}
