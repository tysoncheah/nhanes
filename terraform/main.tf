import {
  to = google_storage_bucket.nhanes
  id = "nhanes-bucket"
}

import {
  to = google_bigquery_dataset.nhanes
  id = "nhanes-493602/nhanes"
}

locals {
  common_labels = merge(
    {
      project     = "nhanes"
      managed_by  = "terraform"
      environment = var.environment
    },
    var.labels
  )

}

resource "google_project_service" "serviceusage" {
  project            = var.project_id
  service            = "serviceusage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "required" {
  for_each = toset([
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false

  depends_on = [google_project_service.serviceusage]
}

resource "google_storage_bucket" "nhanes" {
  name                        = var.bucket_name
  location                    = var.location
  storage_class               = var.bucket_storage_class
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy_bucket
  labels                      = local.common_labels

  versioning {
    enabled = var.bucket_versioning_enabled
  }

  depends_on = [google_project_service.required["storage.googleapis.com"]]
}

resource "google_bigquery_dataset" "nhanes" {
  dataset_id                 = var.dataset_id
  location                   = var.location
  description                = "NHANES raw, staged, and analytical tables managed by Terraform."
  delete_contents_on_destroy = false
  labels                     = local.common_labels

  depends_on = [google_project_service.required["bigquery.googleapis.com"]]
}



# Match the current Kestra bootstrap subflow by default, while allowing a tighter
# runtime-only mode once the bucket and dataset are managed exclusively by Terraform.


resource "google_bigquery_dataset_iam_member" "kestra_dataset_editor" {
  dataset_id = google_bigquery_dataset.nhanes.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.kestra_service_account_email}"
}



resource "google_storage_bucket_iam_member" "kestra_bucket_object_admin" {
  count = var.enable_kestra_bootstrap_permissions ? 0 : 1

  bucket = google_storage_bucket.nhanes.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.kestra_service_account_email}"
}
