with source as (
    select * from {{ source('nhanes_landing', 'glu') }}
)

select
    concat(cast(seqn as string), '|', cast(cycle_start_year as string), '|', cast(cycle_end_year as string)) as respondent_key,
    safe_cast(seqn as int64) as respondent_id,
    safe_cast(cycle_start_year as int64) as cycle_start_year,
    safe_cast(cycle_end_year as int64) as cycle_end_year,
    cast(cycle_label as string) as cycle_label,
    safe_cast(wtsaf2yr as numeric) as fasting_weight_2yr,
    safe_cast(lbxglu as numeric) as fasting_glucose_mg_dl,
    safe_cast(lbdglusi as numeric) as fasting_glucose_mmol_l,
    safe_cast(phafsthr as numeric) as fasting_hours,
    safe_cast(phafstmn as numeric) as fasting_minutes,
    safe_cast(lbxin as numeric) as fasting_insulin_uuml,
    safe_cast(lbdinsi as numeric) as fasting_insulin_pmol_l,
    cast(file_code as string) as file_code,
    cast(source_url as string) as source_url
from source
