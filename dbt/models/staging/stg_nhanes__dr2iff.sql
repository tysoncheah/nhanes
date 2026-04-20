with source as (
    select * from {{ source('nhanes_landing', 'dr2iff') }}
),
base as (
    select
        concat(cast(seqn as string), '|', cast(cycle_start_year as string), '|', cast(cycle_end_year as string)) as respondent_key,
        concat(
            cast(seqn as string),
            '|',
            cast(cycle_start_year as string),
            '|',
            cast(cycle_end_year as string),
            '|2|',
            cast(dr2iline as string)
        ) as protein_item_key,
        safe_cast(seqn as int64) as respondent_id,
        safe_cast(cycle_start_year as int64) as cycle_start_year,
        safe_cast(cycle_end_year as int64) as cycle_end_year,
        cast(cycle_label as string) as cycle_label,
        safe_cast(dr2iline as int64) as food_line_number,
        safe_cast(dr2ifdcd as int64) as food_code,
        safe_cast(wtdr2d as numeric) as day_2_recall_weight_2yr,
        safe_cast(dr2igrms as numeric) as intake_grams,
        safe_cast(dr2ikcal as numeric) as energy_kcal,
        safe_cast(dr2iprot as numeric) as protein_g,
        safe_cast(dr2icarb as numeric) as carb_g,
        safe_cast(dr2itfat as numeric) as fat_g,
        cast(dr2_030z as string) as eating_occasion,
        cast(dr2_040z as string) as ate_at_home_response,
        cast(file_code as string) as file_code,
        cast(source_url as string) as source_url
    from source
)

select
    protein_item_key,
    respondent_key,
    respondent_id,
    cycle_start_year,
    cycle_end_year,
    cycle_label,
    food_line_number,
    food_code,
    case
        when food_code is not null then lpad(cast(food_code as string), 8, '0')
    end as food_code_padded,
    safe_cast(
        substr(
            case
                when food_code is not null then lpad(cast(food_code as string), 8, '0')
            end,
            1,
            1
        ) as int64
    ) as food_code_major_group,
    day_2_recall_weight_2yr,
    intake_grams,
    energy_kcal,
    protein_g,
    carb_g,
    fat_g,
    eating_occasion,
    ate_at_home_response,
    file_code,
    source_url
from base
