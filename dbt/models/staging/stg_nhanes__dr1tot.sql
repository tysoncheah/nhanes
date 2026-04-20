with source as (
    select * from {{ source('nhanes_landing', 'dr1tot') }}
)

select
    concat(cast(seqn as string), '|', cast(cycle_start_year as string), '|', cast(cycle_end_year as string)) as respondent_key,
    safe_cast(seqn as int64) as respondent_id,
    safe_cast(cycle_start_year as int64) as cycle_start_year,
    safe_cast(cycle_end_year as int64) as cycle_end_year,
    cast(cycle_label as string) as cycle_label,
    safe_cast(wtdrd1 as numeric) as day_1_recall_weight_2yr,
    safe_cast(dr1drstz as int64) as day_1_recall_status,
    safe_cast(dr1tkcal as numeric) as day_1_energy_kcal,
    safe_cast(dr1tprot as numeric) as day_1_protein_g,
    safe_cast(dr1tcarb as numeric) as day_1_carb_g,
    safe_cast(dr1ttfat as numeric) as day_1_fat_g,
    cast(file_code as string) as file_code,
    cast(source_url as string) as source_url
from source
