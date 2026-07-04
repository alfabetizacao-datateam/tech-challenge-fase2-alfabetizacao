resource "google_dataproc_cluster" "spark_cluster" {
  name    = var.cluster_name
  region  = var.region
  project = var.project_id

  cluster_config {
    gce_cluster_config {
      zone = "us-central1-b"
    }
    master_config {
      num_instances = 1
      machine_type  = var.machine_type

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = var.num_workers
      machine_type  = var.machine_type

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    software_config {
      image_version = "2.2-debian12"
    }

    lifecycle_config {
      idle_delete_ttl = "1800s"
    }
  }
}
