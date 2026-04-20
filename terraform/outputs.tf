output "bucket_name" {
  description = "GCS bucket name to store NHANES raw and staged assets."
  value       = google_storage_bucket.nhanes.name
}

output "project_id" {
  description = "GCP project ID used by the NHANES platform."
  value       = var.project_id
}

output "location" {
  description = "Shared GCP location used by Kestra plugin defaults."
  value       = var.location
}

output "dataset_id" {
  description = "BigQuery dataset for NHANES tables."
  value       = google_bigquery_dataset.nhanes.dataset_id
}

output "kestra_service_account_email" {
  description = "Service account email to use for Kestra GCP authentication."
  value       = var.kestra_service_account_email
}

output "kestra_kv_values" {
  description = "Values that align with the current Kestra KV keys."
  value = {
    GCP_PROJECT_ID  = var.project_id
    GCP_LOCATION    = var.location
    GCP_BUCKET_NAME = google_storage_bucket.nhanes.name
    GCP_DATASET     = google_bigquery_dataset.nhanes.dataset_id
  }
}
