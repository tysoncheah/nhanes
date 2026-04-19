with respondent_daily as (
    select * from {{ ref('int_nhanes__respondent_daily_nutrients') }}
),
protein_sources as (
    select * from {{ ref('int_nhanes__respondent_protein_source_estimates') }}
),
joined as (
    select
        d.respondent_key,
        d.respondent_id,
        d.cycle_start_year,
        d.cycle_end_year,
        d.cycle_label,
        d.survey_cycle_code,
        d.age_years,
        d.age_band_levine,
        d.sex_code,
        d.race_ethnicity_code,
        d.education_code,
        d.poverty_income_ratio,
        d.survey_psu,
        d.survey_strata,
        d.interview_weight_2yr,
        d.mec_weight_2yr,
        d.day_1_recall_weight_2yr,
        d.day_2_recall_weight_2yr,
        d.fasting_weight_2yr,
        d.day_1_energy_kcal,
        d.day_1_protein_g,
        d.day_1_carb_g,
        d.day_1_fat_g,
        d.day_2_energy_kcal,
        d.day_2_protein_g,
        d.day_2_carb_g,
        d.day_2_fat_g,
        d.fasting_glucose_mg_dl,
        d.fasting_glucose_mmol_l,
        d.fasting_insulin_uuml,
        d.fasting_insulin_pmol_l,
        d.fasting_hours,
        d.fasting_minutes,
        d.mortality_status,
        d.died_during_followup,
        d.underlying_cause_group_code,
        d.diabetes_mentioned_flag,
        d.hypertension_mentioned_flag,
        d.followup_months_interview,
        d.followup_months_exam,
        d.mortality_public_release_year,
        p.day_1_item_protein_g,
        p.day_1_item_energy_kcal,
        p.day_1_animal_protein_g_est,
        p.day_1_plant_protein_g_est,
        p.day_1_unclassified_protein_g,
        p.day_1_animal_protein_share_est,
        p.day_1_plant_protein_share_est,
        p.day_1_unclassified_protein_share,
        p.day_2_item_protein_g,
        p.day_2_item_energy_kcal,
        p.day_2_animal_protein_g_est,
        p.day_2_plant_protein_g_est,
        p.day_2_unclassified_protein_g,
        p.day_2_animal_protein_share_est,
        p.day_2_plant_protein_share_est,
        p.day_2_unclassified_protein_share
    from respondent_daily as d
    left join protein_sources as p
        using (respondent_key)
    where d.mortality_eligible_flag
      and d.age_years >= 50
      and d.day_1_energy_kcal is not null
      and d.day_1_protein_g is not null
)

select
    respondent_key,
    respondent_id,
    cycle_start_year,
    cycle_end_year,
    cycle_label,
    survey_cycle_code,
    age_years,
    age_band_levine,
    sex_code,
    race_ethnicity_code,
    education_code,
    poverty_income_ratio,
    survey_psu,
    survey_strata,
    interview_weight_2yr,
    mec_weight_2yr,
    day_1_recall_weight_2yr,
    day_2_recall_weight_2yr,
    fasting_weight_2yr,
    safe_divide(mec_weight_2yr, 8.0) as mec_weight_16yr,
    safe_divide(day_1_recall_weight_2yr, 8.0) as day_1_recall_weight_16yr,
    safe_divide(day_2_recall_weight_2yr, 8.0) as day_2_recall_weight_16yr,
    safe_divide(fasting_weight_2yr, 8.0) as fasting_weight_16yr,
    day_1_energy_kcal,
    day_1_protein_g,
    day_1_carb_g,
    day_1_fat_g,
    safe_divide(day_1_protein_g * 4.0, nullif(day_1_energy_kcal, 0)) as protein_pct_kcal_day_1,
    case
        when safe_divide(day_1_protein_g * 4.0, nullif(day_1_energy_kcal, 0)) < 0.10 then 'LOW'
        when safe_divide(day_1_protein_g * 4.0, nullif(day_1_energy_kcal, 0)) < 0.20 then 'MODERATE'
        else 'HIGH'
    end as protein_group_day_1,
    day_2_energy_kcal,
    day_2_protein_g,
    day_2_carb_g,
    day_2_fat_g,
    case
        when day_2_energy_kcal is not null and day_2_protein_g is not null
            then safe_divide(
                ((day_1_protein_g + day_2_protein_g) / 2.0) * 4.0,
                nullif((day_1_energy_kcal + day_2_energy_kcal) / 2.0, 0)
            )
    end as protein_pct_kcal_mean_2day,
    day_1_item_protein_g,
    day_1_item_energy_kcal,
    day_1_animal_protein_g_est,
    day_1_plant_protein_g_est,
    day_1_unclassified_protein_g,
    day_1_animal_protein_share_est,
    day_1_plant_protein_share_est,
    day_1_unclassified_protein_share,
    safe_divide(day_1_animal_protein_g_est * 4.0, nullif(day_1_energy_kcal, 0)) as day_1_animal_protein_pct_kcal_est,
    safe_divide(day_1_plant_protein_g_est * 4.0, nullif(day_1_energy_kcal, 0)) as day_1_plant_protein_pct_kcal_est,
    day_2_item_protein_g,
    day_2_item_energy_kcal,
    day_2_animal_protein_g_est,
    day_2_plant_protein_g_est,
    day_2_unclassified_protein_g,
    day_2_animal_protein_share_est,
    day_2_plant_protein_share_est,
    day_2_unclassified_protein_share,
    fasting_glucose_mg_dl,
    fasting_glucose_mmol_l,
    fasting_insulin_uuml,
    fasting_insulin_pmol_l,
    fasting_hours,
    fasting_minutes,
    case
        when fasting_glucose_mg_dl >= 126 then 'DIABETES_RANGE'
        when fasting_glucose_mg_dl >= 100 then 'PREDIABETES_RANGE'
        when fasting_glucose_mg_dl is null then 'MISSING'
        else 'NORMAL_RANGE'
    end as fasting_glucose_category,
    mortality_status,
    died_during_followup,
    cast(underlying_cause_group_code = '002' as bool) as cancer_death_flag,
    cast(diabetes_mentioned_flag = 1 as bool) as diabetes_mcod_flag,
    cast(hypertension_mentioned_flag = 1 as bool) as hypertension_mcod_flag,
    underlying_cause_group_code,
    followup_months_interview,
    followup_months_exam,
    mortality_public_release_year,
    'Original paper used NHANES III rather than continuous NHANES 2003-2018; this mart is a validation extension rather than an exact replication.' as comparability_note,
    'Protein-source estimation uses broad USDA FNDDS major food groups. Mixed or ambiguous categories remain partially unclassified until a richer food-code mapping is added.' as protein_source_note
from joined
