import streamlit as st
import pandas as pd
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

st.set_page_config(
    page_title="NHANES Low Protein Validation",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────
#  GLOBAL CSS
# ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

/* ── Root / Background ── */
html, body, [data-testid="stAppViewContainer"], .stApp {
    background-color: #050810 !important;
    font-family: 'Inter', sans-serif;
    color: #c9d1e0;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0a0f1e 0%, #050810 100%) !important;
    border-right: 1px solid rgba(56, 189, 248, 0.15);
}
[data-testid="stSidebar"]::before {
    content: '';
    position: fixed;
    top: 0; left: 0;
    width: 260px; height: 100vh;
    background: radial-gradient(ellipse at 30% 20%, rgba(56,189,248,0.07) 0%, transparent 65%);
    pointer-events: none;
    z-index: 0;
}

/* ── Sidebar avatar / name block ── */
.profile-card {
    text-align: center;
    padding: 2rem 1rem 1.5rem;
    position: relative;
}
.avatar-ring {
    display: inline-block;
    width: 84px; height: 84px;
    border-radius: 50%;
    background: linear-gradient(135deg, #38bdf8, #818cf8, #f472b6);
    padding: 3px;
    box-shadow: 0 0 24px rgba(56,189,248,0.55), 0 0 60px rgba(56,189,248,0.18);
    margin-bottom: .75rem;
}
.avatar-inner {
    width: 100%; height: 100%;
    border-radius: 50%;
    background: #0a0f1e;
    display: flex; align-items: center; justify-content: center;
    font-size: 2rem;
}
.profile-name {
    font-size: 1.05rem;
    font-weight: 700;
    color: #f1f5f9;
    letter-spacing: .02em;
    margin: 0;
}
.profile-title {
    font-size: .75rem;
    color: #64748b;
    margin-top: .25rem;
    letter-spacing: .04em;
    text-transform: uppercase;
}

/* ── Social link buttons ── */
.social-links {
    display: flex;
    flex-direction: column;
    gap: .55rem;
    padding: .25rem 1.25rem 1.5rem;
}
.social-btn {
    display: flex;
    align-items: center;
    gap: .6rem;
    padding: .55rem .9rem;
    border-radius: 10px;
    text-decoration: none !important;
    font-size: .8rem;
    font-weight: 600;
    letter-spacing: .02em;
    transition: all .25s ease;
    border: 1px solid rgba(255,255,255,0.07);
}
.social-btn:hover {
    transform: translateX(4px);
    border-color: rgba(56,189,248,0.4);
    box-shadow: 0 0 18px rgba(56,189,248,0.2);
}
.social-btn.website {
    background: linear-gradient(135deg, rgba(56,189,248,0.12), rgba(99,102,241,0.12));
    color: #7dd3fc !important;
}
.social-btn.linkedin {
    background: linear-gradient(135deg, rgba(10,102,194,0.18), rgba(56,189,248,0.08));
    color: #93c5fd !important;
}
.social-btn svg { flex-shrink: 0; }

/* ── Sidebar divider ── */
.sidebar-divider {
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(56,189,248,0.25), transparent);
    margin: .5rem 1.25rem 1rem;
}

/* ── Sidebar section label ── */
.sidebar-label {
    font-size: .68rem;
    font-weight: 700;
    letter-spacing: .1em;
    text-transform: uppercase;
    color: #475569;
    padding: 0 1.25rem .5rem;
}

/* ── Sidebar metric pill ── */
.sidebar-stat {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: .4rem 1.25rem;
    font-size: .78rem;
    color: #94a3b8;
}
.sidebar-stat-val {
    color: #38bdf8;
    font-weight: 700;
    font-size: .82rem;
}

/* ── Main page header ── */
.page-hero {
    padding: 1.8rem 0 .5rem;
    position: relative;
}
.hero-eyebrow {
    font-size: .72rem;
    font-weight: 700;
    letter-spacing: .15em;
    text-transform: uppercase;
    color: #38bdf8;
    margin-bottom: .4rem;
}
.hero-title {
    font-size: 2rem;
    font-weight: 800;
    color: #f1f5f9;
    line-height: 1.2;
    margin: 0 0 .5rem;
    text-shadow: 0 0 40px rgba(56,189,248,0.35);
}
.hero-sub {
    font-size: .9rem;
    color: #64748b;
    max-width: 620px;
    line-height: 1.6;
}

/* ── Glowing section cards ── */
.section-card {
    background: linear-gradient(135deg, rgba(15,23,42,0.9), rgba(10,15,30,0.9));
    border: 1px solid rgba(56,189,248,0.12);
    border-radius: 16px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 0 40px rgba(56,189,248,0.05), inset 0 1px 0 rgba(255,255,255,0.04);
    position: relative;
    overflow: hidden;
}
.section-card::before {
    content: '';
    position: absolute;
    top: -1px; left: 20px; right: 20px; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(56,189,248,0.5), transparent);
}
.section-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0 0 .35rem;
}
.section-desc {
    font-size: .8rem;
    color: #64748b;
    line-height: 1.55;
    margin: 0;
}

/* ── Insight callout ── */
.insight-box {
    background: linear-gradient(135deg, rgba(56,189,248,0.07), rgba(99,102,241,0.06));
    border-left: 3px solid #38bdf8;
    border-radius: 0 10px 10px 0;
    padding: .8rem 1rem;
    margin: .75rem 0 1.1rem;
    font-size: .82rem;
    color: #94a3b8;
    line-height: 1.6;
}
.insight-box strong { color: #7dd3fc; }

/* ── Chart glow wrapper ── */
[data-testid="stPlotlyChart"] {
    border-radius: 12px;
    filter: drop-shadow(0 0 22px rgba(56, 189, 248, 0.18));
}

/* ── Streamlit widget overrides ── */
[data-testid="stMetricValue"] { color: #38bdf8 !important; font-weight: 800 !important; }
[data-testid="stMetricLabel"] { color: #64748b !important; }
div[data-testid="metric-container"] {
    background: linear-gradient(135deg, rgba(15,23,42,0.85), rgba(10,15,30,0.85));
    border: 1px solid rgba(56,189,248,0.13);
    border-radius: 12px;
    padding: .9rem 1.1rem;
    box-shadow: 0 0 20px rgba(56,189,248,0.06);
}

/* ── Info / warning banners ── */
[data-testid="stAlert"] {
    background: rgba(56,189,248,0.06) !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    border-radius: 10px !important;
    color: #94a3b8 !important;
}

/* ── Horizontal divider ── */
hr { border-color: rgba(56,189,248,0.12) !important; }

/* ── Selectbox / radio ── */
[data-testid="stSelectbox"] > div > div {
    background: rgba(15,23,42,0.8) !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    border-radius: 8px !important;
    color: #e2e8f0 !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: #050810; }
::-webkit-scrollbar-thumb { background: rgba(56,189,248,0.25); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(56,189,248,0.45); }

/* ── Footer tag ── */
.footer-tag {
    text-align: center;
    font-size: .72rem;
    color: #334155;
    padding: 2rem 0 1rem;
    letter-spacing: .04em;
}
.footer-tag a { color: #38bdf8 !important; text-decoration: none; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  SIDEBAR
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="profile-card">
        <div class="avatar-ring">
            <div class="avatar-inner">🧬</div>
        </div>
        <p class="profile-name">Tyson Cheah</p>
        <p class="profile-title">Data Analyst · NHANES Research</p>
    </div>

    <div class="social-links">
        <a href="https://tysoncheah.com/" target="_blank" class="social-btn website">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/>
                <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
            </svg>
            tysoncheah.com
        </a>
        <a href="https://www.linkedin.com/in/tyson-cheah/" target="_blank" class="social-btn linkedin">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                <path d="M16 8a6 6 0 0 1 6 6v7h-4v-7a2 2 0 0 0-2-2 2 2 0 0 0-2 2v7h-4v-7a6 6 0 0 1 6-6z"/>
                <rect x="2" y="9" width="4" height="12"/><circle cx="4" cy="4" r="2"/>
            </svg>
            LinkedIn Profile
        </a>
    </div>

    <div class="sidebar-divider"></div>
    <div class="sidebar-label">Study Overview</div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="sidebar-stat">
        <span>Cohort</span><span class="sidebar-stat-val">NHANES 2003–18</span>
    </div>
    <div class="sidebar-stat">
        <span>Reference</span><span class="sidebar-stat-val">Levine 2014</span>
    </div>
    <div class="sidebar-stat">
        <span>Follow-up</span><span class="sidebar-stat-val">Up to 16 yrs</span>
    </div>
    <div class="sidebar-stat">
        <span>Age Groups</span><span class="sidebar-stat-val">50–65 / 66+</span>
    </div>

    <div class="sidebar-divider"></div>
    <div class="sidebar-label">Protein Groups</div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="sidebar-stat"><span>🔵 Low</span><span class="sidebar-stat-val">&lt;10% kcal</span></div>
    <div class="sidebar-stat"><span>🟡 Moderate</span><span class="sidebar-stat-val">10–19%</span></div>
    <div class="sidebar-stat"><span>🔴 High</span><span class="sidebar-stat-val">≥20% kcal</span></div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="padding:.25rem 1.25rem 0; font-size:.72rem; color:#334155; line-height:1.6">
        Mortality rates are weighted per-1,000 using NHANES 16-yr MEC exam weights.
        Protein groupings follow Levine et al. (2014) thresholds.
    </div>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  DATA LOADING
# ──────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    df_mortality = None
    df_protein_disease = None
    try:
        client = bigquery.Client(credentials=credentials, project="nhanes-493602")
        query = """
            SELECT 
                age_band_levine as `Age Group`,
                protein_group_day_1 as `Protein Intake`,
                SUM(case when died_during_followup then day_1_recall_weight_16yr else 0 end) / SUM(day_1_recall_weight_16yr) * 1000 as `All-Cause Mortality`,
                SUM(case when cancer_death_flag then day_1_recall_weight_16yr else 0 end) / SUM(day_1_recall_weight_16yr) * 1000 as `Cancer`,
                SUM(case when diabetes_mcod_flag then day_1_recall_weight_16yr else 0 end) / SUM(day_1_recall_weight_16yr) * 1000 as `Diabetes`,
                SUM(case when underlying_cause_group_code in ('001', '005') then day_1_recall_weight_16yr else 0 end) / SUM(day_1_recall_weight_16yr) * 1000 as `CVD`
            FROM `nhanes.mart_nhanes__validation_cohort`
            WHERE age_band_levine IS NOT NULL AND protein_group_day_1 IS NOT NULL
            GROUP BY 1, 2
        """
        df = client.query(query).to_dataframe()
        df_mortality = pd.melt(df, id_vars=['Age Group', 'Protein Intake'], var_name='Cause', value_name='Mortality Rate (per 1000)')
        df_mortality['Age Group'] = df_mortality['Age Group'].replace({'50_65': '50-65', '66_PLUS': '66+'})
    except Exception as e:
        st.warning(f"Could not fetch from BigQuery ({e}). Using mock data derived from the Levine 2014 paper.")
        mortality_data = []
        causes = ["All-Cause Mortality", "Cancer", "Diabetes", "CVD"]
        rates_50_65 = {"LOW": [50, 10, 2, 15], "MODERATE": [80, 25, 5, 25], "HIGH": [150, 40, 10, 40]}
        rates_66_plus = {"LOW": [250, 50, 15, 80], "MODERATE": [200, 40, 10, 60], "HIGH": [150, 30, 8, 50]}
        for protein, rates in rates_50_65.items():
            for cause, rate in zip(causes, rates):
                mortality_data.append({"Age Group": "50-65", "Protein Intake": protein, "Cause": cause, "Mortality Rate (per 1000)": rate})
        for protein, rates in rates_66_plus.items():
            for cause, rate in zip(causes, rates):
                mortality_data.append({"Age Group": "66+", "Protein Intake": protein, "Cause": cause, "Mortality Rate (per 1000)": rate})
        df_mortality = pd.DataFrame(mortality_data)

    try:
        client = bigquery.Client(credentials=credentials, project="nhanes-493602")
        protein_query = """
            WITH animal_protein_analysis AS (
                SELECT 
                    age_band_levine,
                    'Animal Protein' as protein_type,
                    CASE 
                        WHEN day_1_animal_protein_g_est < 25 THEN 'Q1 (Low)'
                        WHEN day_1_animal_protein_g_est < 35 THEN 'Q2'
                        WHEN day_1_animal_protein_g_est < 45 THEN 'Q3'
                        ELSE 'Q4 (High)'
                    END as protein_quartile,
                    SUM(CASE WHEN cancer_death_flag THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as cancer_rate,
                    SUM(CASE WHEN diabetes_mcod_flag THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as diabetes_rate,
                    SUM(CASE WHEN hypertension_mcod_flag THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as hypertension_rate,
                    SUM(CASE WHEN died_during_followup THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as mortality_rate
                FROM `nhanes.mart_nhanes__validation_cohort`
                WHERE age_band_levine IS NOT NULL AND day_1_animal_protein_g_est IS NOT NULL
                GROUP BY age_band_levine, protein_quartile
            ),
            plant_protein_analysis AS (
                SELECT 
                    age_band_levine,
                    'Plant Protein' as protein_type,
                    CASE 
                        WHEN day_1_plant_protein_g_est < 10 THEN 'Q1 (Low)'
                        WHEN day_1_plant_protein_g_est < 15 THEN 'Q2'
                        WHEN day_1_plant_protein_g_est < 20 THEN 'Q3'
                        ELSE 'Q4 (High)'
                    END as protein_quartile,
                    SUM(CASE WHEN cancer_death_flag THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as cancer_rate,
                    SUM(CASE WHEN diabetes_mcod_flag THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as diabetes_rate,
                    SUM(CASE WHEN hypertension_mcod_flag THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as hypertension_rate,
                    SUM(CASE WHEN died_during_followup THEN day_1_recall_weight_16yr ELSE 0 END) /
                        NULLIF(SUM(day_1_recall_weight_16yr), 0) * 1000 as mortality_rate
                FROM `nhanes.mart_nhanes__validation_cohort`
                WHERE age_band_levine IS NOT NULL AND day_1_plant_protein_g_est IS NOT NULL
                GROUP BY age_band_levine, protein_quartile
            )
            SELECT * FROM animal_protein_analysis
            UNION ALL
            SELECT * FROM plant_protein_analysis
        """
        df_protein = client.query(protein_query).to_dataframe()
        if df_protein is not None and len(df_protein) > 0:
            df_protein_disease = pd.melt(
                df_protein,
                id_vars=['age_band_levine', 'protein_type', 'protein_quartile'],
                value_vars=['cancer_rate', 'diabetes_rate', 'hypertension_rate', 'mortality_rate'],
                var_name='Disease',
                value_name='Rate (per 1000)'
            )
            df_protein_disease['age_band_levine'] = df_protein_disease['age_band_levine'].replace({'50_65': '50-65', '66_PLUS': '66+'})
            df_protein_disease['Disease'] = df_protein_disease['Disease'].str.replace('_rate', '').str.replace('_', ' ').str.title()
    except Exception as e:
        st.warning(f"Could not fetch protein-disease data from BigQuery ({e}).")
        df_protein_disease = None

    return df_mortality, df_protein_disease


df_mortality, df_protein_disease = load_data()

df_mortality['Protein Intake'] = pd.Categorical(df_mortality['Protein Intake'], categories=["LOW", "MODERATE", "HIGH"], ordered=True)
df_mortality = df_mortality.sort_values('Protein Intake')


# ──────────────────────────────────────────────────────────────
#  MAIN CONTENT
# ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="page-hero">
    <div class="hero-eyebrow">🧬 Epidemiology · Nutrition Science</div>
    <h1 class="hero-title">NHANES 2003–2018<br>Low Protein Validation</h1>
    <p class="hero-sub">
        Replication of Levine et al. (2014) findings on dietary protein, IGF-1,
        cancer risk, and all-cause mortality across a 16-year longitudinal cohort.
    </p>
</div>
""", unsafe_allow_html=True)

# ── KPI row ──
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Cohort Cycles", "8 cycles", "2003 – 2018")
with k2:
    st.metric("Follow-up", "Up to 16 yrs", "MEC weighted")
with k3:
    st.metric("Age Groups", "2 bands", "50–65 · 66+")
with k4:
    st.metric("Protein Bands", "3 groups", "Low · Mod · High")

st.markdown("<br>", unsafe_allow_html=True)

# ── Section 1 ──
st.markdown("""
<div class="section-card">
    <p class="section-title">📈 Mortality & Disease Risk by Protein Intake</p>
    <p class="section-desc">
        Weighted mortality rates per 1,000 across three protein intake groups, stratified by age band and cause of death.
    </p>
    <div class="insight-box">
        <strong>Key reversal finding:</strong> For adults aged 50–65, high protein intake is associated with <strong>elevated</strong> mortality and cancer risk.
        Among adults 66+, the relationship <strong>inverts</strong> — higher protein becomes protective.
        This age-dependent reversal is the central claim of Levine 2014.
    </div>
</div>
""", unsafe_allow_html=True)

PALETTE = ["#f43f5e", "#06b6d4"]

fig_line = px.line(
    df_mortality,
    x="Protein Intake",
    y="Mortality Rate (per 1000)",
    color="Age Group",
    facet_col="Cause",
    markers=True,
    color_discrete_sequence=PALETTE,
)
fig_line.update_yaxes(matches=None)
fig_line.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1], font_size=12, font_color="#94a3b8"))
fig_line.update_layout(
    template="plotly_dark",
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(5,8,16,0)",
    font=dict(family="Inter", size=13, color="#94a3b8"),
    hovermode="x unified",
    margin=dict(t=35, b=10, l=10, r=10),
    legend=dict(
        bgcolor="rgba(10,15,30,0.7)",
        bordercolor="rgba(56,189,248,0.2)",
        borderwidth=1,
        font=dict(size=12),
    ),
)
fig_line.update_traces(
    line=dict(width=3),
    marker=dict(size=9, symbol="circle", line=dict(width=2, color="rgba(255,255,255,0.6)")),
)
st.plotly_chart(fig_line, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Section 2 ──
st.markdown("""
<div class="section-card">
    <p class="section-title">🥩🌱 Animal vs Plant Protein · Disease Risk by Quartile</p>
    <p class="section-desc">
        Disease rates broken down by animal and plant protein intake quartiles. Explore whether source of protein
        modifies the mortality risk relationship observed in the primary analysis.
    </p>
    <div class="insight-box">
        <strong>Hypothesis:</strong> Levine 2014 suggests that the deleterious effects of high protein in middle age
        are primarily driven by <strong>animal-sourced protein</strong> via IGF-1 upregulation,
        while plant protein shows a more neutral or beneficial profile.
    </div>
</div>
""", unsafe_allow_html=True)

if df_protein_disease is not None:
    col1, col2 = st.columns(2, gap="medium")

    def _bar_chart(df, x_label):
        fig = px.bar(
            df,
            x='protein_quartile',
            y='Rate (per 1000)',
            color='age_band_levine',
            facet_col='Disease',
            facet_col_wrap=2,
            color_discrete_sequence=PALETTE,
            labels={'protein_quartile': x_label, 'age_band_levine': 'Age Group'},
            barmode='group',
        )
        fig.update_layout(
            template="plotly_dark",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(5,8,16,0)",
            font=dict(family="Inter", size=11, color="#94a3b8"),
            margin=dict(t=45, b=10, l=5, r=5),
            legend=dict(
                bgcolor="rgba(10,15,30,0.7)",
                bordercolor="rgba(56,189,248,0.2)",
                borderwidth=1,
            ),
        )
        fig.update_traces(marker_line_color='rgba(255,255,255,0.15)', marker_line_width=1, opacity=0.92)
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1], font_size=11, font_color="#64748b"))
        return fig

    with col1:
        st.markdown("""
        <div style="font-size:.88rem; font-weight:700; color:#f87171; margin-bottom:.6rem;
                    letter-spacing:.03em; padding:.4rem .8rem; background:rgba(248,113,113,0.08);
                    border-radius:8px; border:1px solid rgba(248,113,113,0.15); display:inline-block;">
            🥩 Animal Protein
        </div>""", unsafe_allow_html=True)
        st.plotly_chart(
            _bar_chart(df_protein_disease[df_protein_disease['protein_type'] == 'Animal Protein'], 'Animal Protein Quartile'),
            use_container_width=True
        )

    with col2:
        st.markdown("""
        <div style="font-size:.88rem; font-weight:700; color:#34d399; margin-bottom:.6rem;
                    letter-spacing:.03em; padding:.4rem .8rem; background:rgba(52,211,153,0.08);
                    border-radius:8px; border:1px solid rgba(52,211,153,0.15); display:inline-block;">
            🌱 Plant Protein
        </div>""", unsafe_allow_html=True)
        st.plotly_chart(
            _bar_chart(df_protein_disease[df_protein_disease['protein_type'] == 'Plant Protein'], 'Plant Protein Quartile'),
            use_container_width=True
        )
else:
    st.warning("Could not load animal and plant protein disease data.")

# ── Footer ──
st.markdown("---")
st.markdown("""
<div class="footer-tag">
    Built by <a href="https://tysoncheah.com/" target="_blank">Tyson Cheah</a> · 
    Data: NHANES 2003–2018 · Reference: Levine et al. 2014 · 
    <a href="https://www.linkedin.com/in/tyson-cheah/" target="_blank">LinkedIn</a>
</div>
""", unsafe_allow_html=True)