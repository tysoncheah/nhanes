variable "project_id" {
  description = "GCP project ID used by the NHANES platform."
  type        = string
  default     = "nhanes-493602"
}

variable "location" {
  description = "Shared GCP location for the bucket, BigQuery dataset, and Kestra plugin defaults."
  type        = string
  default     = "asia-southeast1"
}

variable "environment" {
  description = "Environment label used in names and resource labels."
  type        = string
  default     = "dev"
}

variable "dataset_id" {
  description = "BigQuery dataset that stores NHANES analytical tables."
  type        = string
  default     = "nhanes"
}

variable "bucket_name" {
  description = "The name of the GCS bucket for NHANES data."
  type        = string
  default     = "nhanes-bucket"
}

variable "bucket_storage_class" {
  description = "Storage class for the NHANES data lake bucket."
  type        = string
  default     = "STANDARD"
}

variable "bucket_versioning_enabled" {
  description = "Whether object versioning should be enabled on the NHANES bucket."
  type        = bool
  default     = false
}

variable "force_destroy_bucket" {
  description = "Whether Terraform may delete non-empty buckets during destroy."
  type        = bool
  default     = false
}

variable "kestra_service_account_email" {
  description = "The email address of the existing service account used by Kestra."
  type        = string
  default     = "admin-292@nhanes-493602.iam.gserviceaccount.com"
}

variable "enable_kestra_bootstrap_permissions" {
  description = "When true, grant broader project roles so the existing Kestra bootstrap flow can create the bucket and dataset if they do not exist."
  type        = bool
  default     = true
}

variable "labels" {
  description = "Additional labels to attach to supported resources."
  type        = map(string)
  default     = {}
}
