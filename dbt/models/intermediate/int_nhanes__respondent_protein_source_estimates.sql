with items as (
    select * from {{ ref('int_nhanes__protein_items') }}
),
aggregated as (
    select
        respondent_key,
        respondent_id,
        cycle_start_year,
        cycle_end_year,
        cycle_label,
        recall_day,
        sum(protein_g) as total_item_protein_g,
        sum(energy_kcal) as total_item_energy_kcal,
        sum(case when protein_source_group = 'animal' then protein_g else 0 end) as animal_protein_g_est,
        sum(case when protein_source_group = 'plant' then protein_g else 0 end) as plant_protein_g_est,
        sum(case when protein_source_group = 'unclassified' then protein_g else 0 end) as unclassified_protein_g,
        sum(case when protein_source_group = 'animal' then 1 else 0 end) as animal_item_count,
        sum(case when protein_source_group = 'plant' then 1 else 0 end) as plant_item_count,
        sum(case when protein_source_group = 'unclassified' then 1 else 0 end) as unclassified_item_count
    from items
    group by 1, 2, 3, 4, 5, 6
),
pivoted as (
    select
        respondent_key,
        respondent_id,
        cycle_start_year,
        cycle_end_year,
        cycle_label,
        max(case when recall_day = 1 then total_item_protein_g end) as day_1_item_protein_g,
        max(case when recall_day = 1 then total_item_energy_kcal end) as day_1_item_energy_kcal,
        max(case when recall_day = 1 then animal_protein_g_est end) as day_1_animal_protein_g_est,
        max(case when recall_day = 1 then plant_protein_g_est end) as day_1_plant_protein_g_est,
        max(case when recall_day = 1 then unclassified_protein_g end) as day_1_unclassified_protein_g,
        max(case when recall_day = 1 then animal_item_count end) as day_1_animal_item_count,
        max(case when recall_day = 1 then plant_item_count end) as day_1_plant_item_count,
        max(case when recall_day = 1 then unclassified_item_count end) as day_1_unclassified_item_count,
        max(case when recall_day = 2 then total_item_protein_g end) as day_2_item_protein_g,
        max(case when recall_day = 2 then total_item_energy_kcal end) as day_2_item_energy_kcal,
        max(case when recall_day = 2 then animal_protein_g_est end) as day_2_animal_protein_g_est,
        max(case when recall_day = 2 then plant_protein_g_est end) as day_2_plant_protein_g_est,
        max(case when recall_day = 2 then unclassified_protein_g end) as day_2_unclassified_protein_g,
        max(case when recall_day = 2 then animal_item_count end) as day_2_animal_item_count,
        max(case when recall_day = 2 then plant_item_count end) as day_2_plant_item_count,
        max(case when recall_day = 2 then unclassified_item_count end) as day_2_unclassified_item_count
    from aggregated
    group by 1, 2, 3, 4, 5
)

select
    respondent_key,
    respondent_id,
    cycle_start_year,
    cycle_end_year,
    cycle_label,
    day_1_item_protein_g,
    day_1_item_energy_kcal,
    day_1_animal_protein_g_est,
    day_1_plant_protein_g_est,
    day_1_unclassified_protein_g,
    safe_divide(day_1_animal_protein_g_est, nullif(day_1_item_protein_g, 0)) as day_1_animal_protein_share_est,
    safe_divide(day_1_plant_protein_g_est, nullif(day_1_item_protein_g, 0)) as day_1_plant_protein_share_est,
    safe_divide(day_1_unclassified_protein_g, nullif(day_1_item_protein_g, 0)) as day_1_unclassified_protein_share,
    day_1_animal_item_count,
    day_1_plant_item_count,
    day_1_unclassified_item_count,
    day_2_item_protein_g,
    day_2_item_energy_kcal,
    day_2_animal_protein_g_est,
    day_2_plant_protein_g_est,
    day_2_unclassified_protein_g,
    safe_divide(day_2_animal_protein_g_est, nullif(day_2_item_protein_g, 0)) as day_2_animal_protein_share_est,
    safe_divide(day_2_plant_protein_g_est, nullif(day_2_item_protein_g, 0)) as day_2_plant_protein_share_est,
    safe_divide(day_2_unclassified_protein_g, nullif(day_2_item_protein_g, 0)) as day_2_unclassified_protein_share,
    day_2_animal_item_count,
    day_2_plant_item_count,
    day_2_unclassified_item_count
from pivoted
