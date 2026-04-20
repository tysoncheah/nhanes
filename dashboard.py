import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os

st.set_page_config(page_title="NHANES Low Protein Validation", layout="wide")
st.title("NHANES 2003-2018 Low Protein Validation")
st.markdown("Validation of the Levine et al. 2014 paper on low protein diets, cancer, and mortality, using continuous NHANES 2003-2018 data.")

@st.cache_data
def load_data():
    try:
        # Assumes BigQuery default credentials are set up
        client = bigquery.Client()
        # In a real scenario, the dataset name would be dynamically injected or loaded from env
        # We query the mart table if dbt has run, or the kestra-generated summary table
        query = """
            SELECT 
                age_band_levine,
                protein_group_day_1,
                respondent_n,
                weighted_cancer_deaths,
                weighted_population_day_1
            FROM `nhanes.mart_nhanes__validation_summary`
        """
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        # Fallback to sample data for the purpose of this demonstration if BQ fails
        st.warning(f"Could not connect to BigQuery ({e}). Using mock data for demonstration.")
        return pd.DataFrame({
            "age_band_levine": ["50_65", "50_65", "50_65", "66_PLUS", "66_PLUS", "66_PLUS"],
            "protein_group_day_1": ["LOW", "MODERATE", "HIGH", "LOW", "MODERATE", "HIGH"],
            "respondent_n": [500, 1500, 500, 400, 1200, 400],
            "weighted_cancer_deaths": [1000, 3000, 5500, 4000, 2500, 1000],
            "weighted_population_day_1": [50000, 150000, 50000, 40000, 120000, 40000]
        })

df = load_data()

# Calculate mortality rate (per 1000)
df['cancer_mortality_rate_per_1000'] = (df['weighted_cancer_deaths'] / df['weighted_population_day_1']) * 1000

# Layout with 2 tiles (columns)
col1, col2 = st.columns(2)

with col1:
    st.subheader("Cancer Mortality Rate by Age & Protein Intake")
    st.markdown("Rate per 1000 weighted population. Notice the reversal of risk after age 65.")
    
    chart_data = df.pivot(index='protein_group_day_1', columns='age_band_levine', values='cancer_mortality_rate_per_1000')
    if set(["LOW", "MODERATE", "HIGH"]).issubset(chart_data.index):
        chart_data = chart_data.reindex(["LOW", "MODERATE", "HIGH"])
    
    st.bar_chart(chart_data)

with col2:
    st.subheader("Cohort Sample Size Distribution")
    st.markdown("Number of respondents in each protein intake group.")
    
    sample_data = df.pivot(index='protein_group_day_1', columns='age_band_levine', values='respondent_n')
    if set(["LOW", "MODERATE", "HIGH"]).issubset(sample_data.index):
        sample_data = sample_data.reindex(["LOW", "MODERATE", "HIGH"])
    
    st.bar_chart(sample_data)

st.markdown("---")
st.markdown("**Context:** *Low Protein Intake is Associated with a Major Reduction in IGF-1, Cancer, and Overall Mortality in the 65 and Younger but Not Older Population* (Levine et al., 2014, Cell Metabolism)")
