with demo as (
    select * from {{ ref('stg_nhanes__demo') }}
),
dr1tot as (
    select * from {{ ref('stg_nhanes__dr1tot') }}
),
dr2tot as (
    select * from {{ ref('stg_nhanes__dr2tot') }}
),
glu as (
    select * from {{ ref('stg_nhanes__glu') }}
),
mortality as (
    select * from {{ ref('stg_nhanes__mortality') }}
)

select
    d.respondent_key,
    d.respondent_id,
    d.cycle_start_year,
    d.cycle_end_year,
    d.cycle_label,
    d.survey_cycle_code,
    d.age_years,
    d.sex_code,
    d.race_ethnicity_code,
    d.education_code,
    d.poverty_income_ratio,
    d.survey_psu,
    d.survey_strata,
    d.interview_weight_2yr,
    d.mec_weight_2yr,
    t1.day_1_recall_weight_2yr,
    t2.day_2_recall_weight_2yr,
    t1.day_1_recall_status,
    t2.day_2_recall_status,
    t1.day_1_energy_kcal,
    t1.day_1_protein_g,
    t1.day_1_carb_g,
    t1.day_1_fat_g,
    t2.day_2_energy_kcal,
    t2.day_2_protein_g,
    t2.day_2_carb_g,
    t2.day_2_fat_g,
    g.fasting_weight_2yr,
    g.fasting_glucose_mg_dl,
    g.fasting_glucose_mmol_l,
    g.fasting_hours,
    g.fasting_minutes,
    g.fasting_insulin_uuml,
    g.fasting_insulin_pmol_l,
    m.eligibility_status,
    cast(m.eligibility_status = 1 as bool) as mortality_eligible_flag,
    m.mortality_status,
    cast(m.mortality_status = 1 as bool) as died_during_followup,
    m.underlying_cause_group_code,
    m.diabetes_mentioned_flag,
    m.hypertension_mentioned_flag,
    m.followup_months_interview,
    m.followup_months_exam,
    m.mortality_public_release_year,
    case
        when d.age_years between 50 and 65 then '50_65'
        when d.age_years >= 66 then '66_PLUS'
        else 'UNDER_50'
    end as age_band_levine,
    case
        when d.age_years between 18 and 49 then '18_49'
        when d.age_years between 50 and 65 then '50_65'
        when d.age_years >= 66 then '66_PLUS'
        else 'UNDER_18'
    end as adult_age_band
from demo as d
left join dr1tot as t1
    using (respondent_key)
left join dr2tot as t2
    using (respondent_key)
left join glu as g
    using (respondent_key)
left join mortality as m
    using (respondent_key)
