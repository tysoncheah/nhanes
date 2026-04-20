# Reproducibility Instructions

This document provides a step-by-step guide to run the end-to-end NHANES low-protein validation data pipeline, apply the necessary BigQuery configurations, and launch the Streamlit dashboard.

## 1. Prerequisites

- Python 3.9+
- Google Cloud Platform (GCP) Account with an active Project
- `gcloud` CLI installed and authenticated (`gcloud auth application-default login`)
- Terraform installed
- Docker & Docker Compose (for running Kestra locally)

## 2. Infrastructure Setup (Terraform)

The GCP infrastructure (storage buckets, BigQuery datasets, and IAM service accounts) is managed via Terraform.

1. Navigate to the terraform directory:
   ```bash
   cd terraform
   ```
2. Copy the variables example to `terraform.tfvars` and update your `project_id`:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```
3. Initialize and apply the infrastructure:
   ```bash
   terraform init
   terraform apply
   ```

## 3. Data Ingestion & Transformation (Kestra & dbt)

### Running Kestra
The Kestra orchestration pipeline extracts raw data from the CDC FTP, loads it into BigQuery, and performs initial staging.

1. Create a `.env` file from the example or `gcp_kv.yaml` providing your BigQuery properties.
2. **Configure Google Credentials in Kestra**:
   Since the service account is manually created (`admin-292@nhanes-493602.iam.gserviceaccount.com`), you need to provide Kestra with its credentials. Follow these steps based on the [Kestra Google Credentials Guide](https://kestra.io/docs/how-to-guides/google-credentials):
   - **Generate a JSON Key**: In the Google Cloud Console, navigate to **IAM & Admin** > **Service Accounts**. Select your service account, go to the **Keys** tab, and create a new JSON key. Download the file.
   - **Add the Secret**: If you are using Kestra Open Source with the default environment variable secret backend, base64 encode the JSON key and add it to your Kestra environment variables as `SECRET_GCP_CREDS`. Alternatively, add it via the Kestra UI under **Variables** if you are storing it as a standard variable.
   - **Update Tasks**: Ensure your GCP tasks in the Kestra flow use the `serviceAccount` property to reference the credentials. For example: `serviceAccount: "{{ secret('GCP_CREDS') }}"` (or `"{{ render(vars.GCP_CREDS) }}"` if using variables).
3. Start Kestra using Docker Compose:
   ```bash
   docker-compose up -d
   ```
3. Open the Kestra UI at `http://localhost:8080`.
4. Import the `kestra/nhanes_low_protein_ingestion.yaml` flow.
5. Trigger the flow.
   > **Note:** The `build_validation_cohort` and `build_validation_summary` BigQuery tasks will automatically create the tables with Integer Range Partitioning and Clustering for performance optimization.

### Running dbt
If you prefer running the transformations and building the analytical marts locally with dbt:

1. Navigate to the `dbt` directory:
   ```bash
   cd dbt
   ```
2. Set up your `profiles.yml`:
   ```bash
   cp profiles.yml.example profiles.yml
   ```
   *Edit the `profiles.yml` to inject your dataset and project credentials if not using environment variables.*
3. Install dependencies and run dbt:
   ```bash
   dbt deps
   dbt run
   ```
   > **Note:** The dbt runs will materialize the `mart_` tables and apply our configured Partitioning (`cycle_start_year`) and Clustering (`age_band_levine`, `protein_group_day_1`).

## 4. Running the Streamlit Dashboard

The Streamlit dashboard allows for the visualization of the data, highlighting the findings of the 2014 Levine paper.

1. Install the Python requirements:
   ```bash
   pip install streamlit pandas google-cloud-bigquery db-dtypes
   ```
2. Make sure you are authenticated with GCP so that BigQuery can be queried:
   ```bash
   gcloud auth application-default login
   ```
3. Start the Streamlit application from the root project directory:
   ```bash
   streamlit run dashboard.py
   ```
4. The dashboard will automatically open in your default browser at `http://localhost:8501`.
   *If the BigQuery connection fails, the dashboard will fall back to using mocked sample data for demonstration purposes.*

## Validation Tests Performed
- **Kestra YAML**: Validated syntax and verified BigQuery DDL parameters.
- **Python Streamlit**: Evaluated syntax and load mechanisms for DataFrames.
- **dbt configs**: Ensured `{{ config(...) }}` blocks were correctly positioned at the top of the SQL files.
