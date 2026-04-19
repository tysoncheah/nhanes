locals {
  common_labels = merge(
    {
      project     = "nhanes"
      managed_by  = "terraform"
      environment = var.environment
    },
    var.labels
  )

  bucket_name = (
    var.bucket_name_override != null && trimspace(var.bucket_name_override) != ""
    ? lower(var.bucket_name_override)
    : lower("${var.project_id}-${var.environment}-nhanes-data")
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
  name                        = local.bucket_name
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

resource "google_service_account" "kestra" {
  account_id   = var.kestra_service_account_id
  display_name = var.kestra_service_account_display_name
  description  = "Runs Kestra flows for NHANES ingestion, storage uploads, and BigQuery jobs."

  depends_on = [google_project_service.required["iam.googleapis.com"]]
}

# Match the current Kestra bootstrap subflow by default, while allowing a tighter
# runtime-only mode once the bucket and dataset are managed exclusively by Terraform.
resource "google_project_iam_member" "kestra_bigquery_user" {
  count = var.enable_kestra_bootstrap_permissions ? 1 : 0

  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_project_iam_member" "kestra_bigquery_job_user" {
  count = var.enable_kestra_bootstrap_permissions ? 0 : 1

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_bigquery_dataset_iam_member" "kestra_dataset_editor" {
  dataset_id = google_bigquery_dataset.nhanes.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_project_iam_member" "kestra_storage_admin" {
  count = var.enable_kestra_bootstrap_permissions ? 1 : 0

  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_storage_bucket_iam_member" "kestra_bucket_object_admin" {
  count = var.enable_kestra_bootstrap_permissions ? 0 : 1

  bucket = google_storage_bucket.nhanes.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.kestra.email}"
}
