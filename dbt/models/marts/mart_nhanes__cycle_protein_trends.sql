with respondent_daily as (
    select * from {{ ref('int_nhanes__respondent_daily_nutrients') }}
),
protein_sources as (
    select * from {{ ref('int_nhanes__respondent_protein_source_estimates') }}
),
scored as (
    select
        d.respondent_key,
        d.cycle_start_year,
        d.cycle_end_year,
        d.cycle_label,
        d.adult_age_band,
        d.day_1_recall_weight_2yr,
        d.day_1_energy_kcal,
        d.day_1_protein_g,
        safe_divide(d.day_1_protein_g * 4.0, nullif(d.day_1_energy_kcal, 0)) as protein_pct_kcal_day_1,
        case
            when safe_divide(d.day_1_protein_g * 4.0, nullif(d.day_1_energy_kcal, 0)) < 0.10 then 'LOW'
            when safe_divide(d.day_1_protein_g * 4.0, nullif(d.day_1_energy_kcal, 0)) < 0.20 then 'MODERATE'
            else 'HIGH'
        end as protein_group_day_1,
        d.fasting_glucose_mg_dl,
        p.day_1_animal_protein_share_est,
        p.day_1_plant_protein_share_est
    from respondent_daily as d
    left join protein_sources as p
        using (respondent_key)
    where d.age_years >= 18
      and d.day_1_energy_kcal is not null
      and d.day_1_protein_g is not null
)

select
    concat(cycle_label, '|', adult_age_band, '|', protein_group_day_1) as trend_key,
    cycle_start_year,
    cycle_end_year,
    cycle_label,
    adult_age_band,
    protein_group_day_1,
    count(*) as respondent_n,
    sum(coalesce(day_1_recall_weight_2yr, 0)) as weighted_population_2yr,
    avg(day_1_energy_kcal) as avg_day_1_energy_kcal,
    avg(day_1_protein_g) as avg_day_1_protein_g,
    avg(protein_pct_kcal_day_1) as avg_protein_pct_kcal_day_1,
    avg(day_1_animal_protein_share_est) as avg_day_1_animal_protein_share_est,
    avg(day_1_plant_protein_share_est) as avg_day_1_plant_protein_share_est,
    avg(fasting_glucose_mg_dl) as avg_fasting_glucose_mg_dl
from scored
where adult_age_band in ('18_49', '50_65', '66_PLUS')
group by 1, 2, 3, 4, 5, 6
