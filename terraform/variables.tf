variable "project_id" {
  description = "GCP project ID used by the NHANES platform."
  type        = string
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

variable "bucket_name_override" {
  description = "Optional globally unique GCS bucket name. Leave null to derive one from project and environment."
  type        = string
  default     = null
  nullable    = true
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

variable "kestra_service_account_id" {
  description = "Account ID for the service account used by Kestra GCP plugins."
  type        = string
  default     = "kestra-nhanes"
}

variable "kestra_service_account_display_name" {
  description = "Display name for the Kestra service account."
  type        = string
  default     = "Kestra NHANES Orchestrator"
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
