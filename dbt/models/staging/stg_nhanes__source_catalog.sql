with source as (
    select * from {{ source('nhanes_landing', 'source_catalog') }}
)

select
    concat(
        cast(asset_type as string),
        '|',
        cast(file_code as string),
        '|',
        cast(cycle_start_year as string),
        '|',
        cast(cycle_end_year as string)
    ) as source_asset_key,
    lower(cast(asset_type as string)) as asset_type,
    cast(file_code as string) as file_code,
    cast(file_format as string) as file_format,
    safe_cast(cycle_start_year as int64) as cycle_start_year,
    safe_cast(cycle_end_year as int64) as cycle_end_year,
    cast(cycle_label as string) as cycle_label,
    cast(source_url as string) as source_url,
    safe_cast(mortality_public_release_year as int64) as mortality_public_release_year,
    cast(mortality_followup_available_through as string) as mortality_followup_available_through,
    cast(paper_comparability_note as string) as paper_comparability_note
from source
