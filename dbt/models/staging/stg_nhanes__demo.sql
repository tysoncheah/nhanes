with source as (
    select * from {{ source('nhanes_landing', 'demo') }}
)

select
    concat(cast(seqn as string), '|', cast(cycle_start_year as string), '|', cast(cycle_end_year as string)) as respondent_key,
    safe_cast(seqn as int64) as respondent_id,
    safe_cast(cycle_start_year as int64) as cycle_start_year,
    safe_cast(cycle_end_year as int64) as cycle_end_year,
    cast(cycle_label as string) as cycle_label,
    safe_cast(sddsrvyr as int64) as survey_cycle_code,
    safe_cast(ridageyr as int64) as age_years,
    safe_cast(riagendr as int64) as sex_code,
    safe_cast(ridreth1 as int64) as race_ethnicity_code,
    safe_cast(dmdeduc2 as int64) as education_code,
    safe_cast(indfmpir as numeric) as poverty_income_ratio,
    safe_cast(sdmvpsu as int64) as survey_psu,
    safe_cast(sdmvstra as int64) as survey_strata,
    safe_cast(wtint2yr as numeric) as interview_weight_2yr,
    safe_cast(wtmec2yr as numeric) as mec_weight_2yr,
    cast(file_code as string) as file_code,
    cast(source_url as string) as source_url
from source
