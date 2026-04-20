with day_1 as (
    select
        protein_item_key,
        respondent_key,
        respondent_id,
        cycle_start_year,
        cycle_end_year,
        cycle_label,
        1 as recall_day,
        food_line_number,
        food_code,
        food_code_padded,
        food_code_major_group,
        day_1_recall_weight_2yr as recall_day_weight_2yr,
        intake_grams,
        energy_kcal,
        protein_g,
        carb_g,
        fat_g,
        eating_occasion,
        ate_at_home_response
    from {{ ref('stg_nhanes__dr1iff') }}
),
day_2 as (
    select
        protein_item_key,
        respondent_key,
        respondent_id,
        cycle_start_year,
        cycle_end_year,
        cycle_label,
        2 as recall_day,
        food_line_number,
        food_code,
        food_code_padded,
        food_code_major_group,
        day_2_recall_weight_2yr as recall_day_weight_2yr,
        intake_grams,
        energy_kcal,
        protein_g,
        carb_g,
        fat_g,
        eating_occasion,
        ate_at_home_response
    from {{ ref('stg_nhanes__dr2iff') }}
),
unioned as (
    select * from day_1
    union all
    select * from day_2
),
classified as (
    select
        u.*,
        coalesce(m.major_group_label, 'Unknown') as major_group_label,
        coalesce(m.protein_source_group, 'unclassified') as protein_source_group,
        coalesce(m.classification_method, 'fndds_major_group_heuristic') as classification_method,
        coalesce(
            m.classification_notes,
            'No major-group mapping was available for this food code.'
        ) as classification_notes
    from unioned as u
    left join {{ ref('food_code_major_group_map') }} as m
        on u.food_code_major_group = m.food_code_major_group
)

select *
from classified
where protein_g is not null
