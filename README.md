# NHANES to BigQuery

## Overview

This project is a cloud-native data engineering platform for replicating and extending nutritional longevity research using NHANES and linked mortality data. The core objective is to operationalize a reproducible pipeline that ingests public health datasets, converts legacy SAS Transport (`.xpt`) files into analytics-friendly Parquet, and models the relationship between protein intake, IGF-1, and mortality outcomes in BigQuery.

The plan is based on a validation workflow inspired by prior nutritional epidemiology research on protein intake and aging. It emphasizes modern data engineering practices, including Infrastructure as Code, workflow orchestration, analytics engineering, and scalable batch processing.

## Project Goals

- Build a reproducible GCP-based data platform with Terraform.
- Ingest NHANES dietary, demographic, laboratory, and mortality-linked datasets.
- Convert CDC XPT files into Parquet for efficient downstream analytics.
- Orchestrate ingestion and transformation with Kestra.
- Model curated analytical datasets in BigQuery with dbt.
- Support both historical mortality validation and current dietary trend analysis.

## Architecture

The platform follows a medallion-style architecture across Google Cloud Storage and BigQuery:

| Layer | Purpose | Format |
| --- | --- | --- |
| Landing / Raw | Initial ingestion of CDC files | `.xpt` |
| Bronze / Processing | Parsed and standardized lakehouse storage | `.parquet` |
| Gold / Analytical | Cleaned, partitioned, analysis-ready datasets | `.parquet` / BigQuery tables |

### Core Components

- **Terraform** provisions cloud resources such as GCS buckets, BigQuery datasets, and IAM configuration.
- **Kestra** orchestrates ingestion, conversion, and loading workflows.
- **Python + pyreadstat** read CDC SAS Transport files while preserving metadata.
- **Parquet** acts as the canonical storage format for transformed data.
- **BigQuery** serves as the analytical warehouse.
- **dbt** builds staging, intermediate, and mart models for downstream analysis.

## Data Sources

The project combines multiple public health datasets and reference tables:

- **NHANES dietary intake files**
  - Example files: `DR1IFF_L.xpt`, `DR2IFF_L.xpt`
  - Used to capture individual food consumption and nutrient intake
- **NHANES demographics**
  - Provides respondent attributes and survey context
- **NHANES laboratory data**
  - Includes serum biomarkers such as IGF-1 for biological mediation analysis
- **Linked Mortality Files (LMF / NDI-linked public-use mortality data)**
  - Used to derive survival and mortality outcomes
- **USDA food coding or food pattern references**
  - Used to classify protein sources as animal-based or plant-based

## Key Analytical Variables

The plan centers on a set of variables that support replication of nutritional longevity analysis:

- `SEQN`: respondent-level join key across NHANES components
- `DR1TPROT`: total protein intake in grams
- `DR1TKCAL`: total energy intake in kilocalories
- `DR1IFDCD`: USDA food code for item-level protein source classification
- `MORTSTAT`: mortality status
- `UCOD_113`: underlying cause-of-death grouping
- `PERMTH_INT`: person-months of follow-up
- `LBXIGF1`: serum IGF-1
- `LBXIGB3`: IGF binding protein 3
- `LBXGLU`: fasting glucose

## End-to-End Workflow

### 1. Provision Infrastructure

Terraform defines the full cloud environment so the project can be recreated consistently across environments. This includes storage buckets, warehouse datasets, and governance-oriented configuration such as location and expiration policies.

### 2. Ingest NHANES XPT Files

Kestra runs Python tasks that pull public NHANES files from CDC endpoints. The ingestion process is designed to be idempotent so reruns do not create duplicate records.

### 3. Convert XPT to Parquet

CDC SAS Transport files are parsed into DataFrames and serialized into Parquet. This step improves compression, preserves schema fidelity, and makes downstream joins and analytics more efficient.

### 4. Load Curated Data into BigQuery

Parquet outputs are stored in cloud storage and loaded into BigQuery, where schema inference and warehouse-native optimizations can be used for analysis.

### 5. Model Data with dbt

dbt transformations are organized into:

- **Staging**
  - Standardize source schemas and cast types
- **Intermediate**
  - Implement protein source classification and respondent-level nutrient logic
- **Marts**
  - Publish analytical tables for mortality validation, cohort analysis, and dashboarding

## Analytical Use Cases

### Historical Mortality Validation

The primary validation path uses historical NHANES cycles with public mortality linkage to reproduce or extend prior findings on protein intake and long-term outcomes.

### Current Dietary Trend Monitoring

Because newer NHANES cycles provide recent intake data without mature mortality follow-up, they can still be used to analyze current protein consumption patterns and estimate future risk through models trained on historical cohorts.

### Biological Mediation Analysis

By joining protein intake with laboratory biomarkers such as IGF-1, the platform can support analysis of whether biological signaling pathways mediate the observed relationship between diet and mortality.

## Important Constraint

The PDF plan highlights a major temporal limitation in the public mortality linkage. Public-use linked mortality follow-up is available for historical cohorts, but the newest NHANES cycles do not yet have mature public mortality linkage for direct validation.

In practice, this means:

- NHANES III and continuous historical cycles through roughly 1999-2018 are the right foundation for mortality outcome validation
- newer cycles such as 2021-2023 are better suited for descriptive intake analysis and projected risk modeling
- the pipeline should be designed so future CDC mortality releases can be integrated without redesigning the platform

## Proposed Outputs

The project is intended to produce:

- respondent-level nutrient profiles
- animal vs. plant protein indices
- protein-energy ratio features
- mortality-linked analytical marts
- age-stratified cohorts for hazard modeling
- IGF-1 mediation-ready research tables

## Implementation Roadmap

1. Provision cloud infrastructure with Terraform.
2. Ingest dietary, demographic, laboratory, and mortality files.
3. Convert raw XPT inputs into Bronze Parquet datasets.
4. Build dbt staging, intermediate, and mart models in BigQuery.
5. Add food-code enrichment to classify animal and plant protein sources.
6. Publish analytical tables for validation and dashboard use.

## Why This Project Matters

This project turns a complex public health research workflow into a repeatable, production-ready data platform. It bridges legacy epidemiology data formats with modern cloud analytics, making it possible to validate nutritional longevity hypotheses, track dietary shifts over time, and support future research as additional linked mortality data becomes available.
