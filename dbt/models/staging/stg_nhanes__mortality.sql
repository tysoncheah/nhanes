with source as (
    select * from {{ source('nhanes_landing', 'mortality') }}
)

select
    concat(cast(seqn as string), '|', cast(cycle_start_year as string), '|', cast(cycle_end_year as string)) as respondent_key,
    safe_cast(seqn as int64) as respondent_id,
    safe_cast(cycle_start_year as int64) as cycle_start_year,
    safe_cast(cycle_end_year as int64) as cycle_end_year,
    cast(cycle_label as string) as cycle_label,
    safe_cast(eligstat as int64) as eligibility_status,
    safe_cast(mortstat as int64) as mortality_status,
    cast(ucod_leading as string) as underlying_cause_group_code,
    safe_cast(diabetes as int64) as diabetes_mentioned_flag,
    safe_cast(hyperten as int64) as hypertension_mentioned_flag,
    safe_cast(permth_int as numeric) as followup_months_interview,
    safe_cast(permth_exm as numeric) as followup_months_exam,
    safe_cast(mortality_public_release_year as int64) as mortality_public_release_year,
    cast(file_code as string) as file_code,
    cast(source_url as string) as source_url
from source
