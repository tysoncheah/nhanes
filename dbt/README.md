# NHANES dbt Project

This dbt project assumes Kestra has already loaded the NHANES landing tables into BigQuery.

## Models

- `staging/`: standardizes the raw BigQuery landing tables
- `intermediate/`: joins respondent-level nutrient, mortality, and fasting-glucose records and estimates broad protein-source splits from item-level food codes
- `marts/`: publishes validation-ready marts for mortality analysis and dashboarding

## Protein Source Logic

The initial classifier uses the first digit of the USDA 8-digit food code to map items into broad FNDDS major food groups. This is intentionally conservative:

- animal-heavy groups are classified as `animal`
- plant-heavy groups are classified as `plant`
- ambiguous groups remain `unclassified`

That keeps the current mart honest while leaving room for a richer seed or reference table later.

## Local Run

1. Copy `profiles.yml.example` to `profiles.yml`
2. Set the required BigQuery environment variables
3. Run `dbt seed --profiles-dir .`
4. Run `dbt run --profiles-dir .`
5. Run `dbt test --profiles-dir .`
