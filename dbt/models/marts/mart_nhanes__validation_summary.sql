with cohort as (
    select * from {{ ref('mart_nhanes__validation_cohort') }}
)

select
    concat(age_band_levine, '|', protein_group_day_1) as summary_key,
    age_band_levine,
    protein_group_day_1,
    count(*) as respondent_n,
    sum(coalesce(day_1_recall_weight_16yr, 0)) as weighted_population_day_1,
    sum(case when died_during_followup then coalesce(day_1_recall_weight_16yr, 0) else 0 end) as weighted_all_cause_deaths,
    sum(case when cancer_death_flag then coalesce(day_1_recall_weight_16yr, 0) else 0 end) as weighted_cancer_deaths,
    sum(case when diabetes_mcod_flag then coalesce(day_1_recall_weight_16yr, 0) else 0 end) as weighted_diabetes_mentions,
    avg(protein_pct_kcal_day_1) as avg_protein_pct_kcal_day_1,
    avg(protein_pct_kcal_mean_2day) as avg_protein_pct_kcal_mean_2day,
    avg(day_1_animal_protein_share_est) as avg_day_1_animal_protein_share_est,
    avg(day_1_plant_protein_share_est) as avg_day_1_plant_protein_share_est,
    avg(fasting_glucose_mg_dl) as avg_fasting_glucose_mg_dl,
    avg(followup_months_interview) as avg_followup_months_interview,
    avg(followup_months_exam) as avg_followup_months_exam
from cohort
group by 1, 2, 3
