import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

@st.cache_data
def load_parquet(name):
    return pd.read_parquet(DATA_DIR / name)

# Load data
core = load_parquet("stunting_core.parquet")
priority = load_parquet("priority_regions.parquet")
trend = load_parquet("trend_national.parquet")

# =====================
# DASHBOARD
# =====================

st.set_page_config(
    page_title="Analisis Big Data Stunting Indonesia",
    layout="wide"
)

st.title("📊 Analisis Big Data Stunting Indonesia")
st.caption(
    "Analisis kesehatan publik berbasis Hadoop (HDFS) dan Spark "
    "untuk mendukung pengambilan keputusan kebijakan."
)

# METRICS
col1, col2, col3 = st.columns(3)

col1.metric(
    "📉 Rata-rata Nasional",
    f"{round(core['prevalensi_stunting'].mean(),2)} %"
)

worst = core.sort_values("prevalensi_stunting", ascending=False).iloc[0]
col2.metric(
    "🚨 Wilayah Terparah",
    worst["kabupaten_kota"],
    f"{worst['prevalensi_stunting']} %"
)

col3.metric(
    "⚠️ Wilayah Prioritas",
    len(priority)
)

st.divider()

# TREND NASIONAL
st.subheader("📈 Tren Nasional Stunting")

fig = px.line(
    trend,
    x="tahun",
    y="avg_prevalensi",
    markers=True
)

st.plotly_chart(fig, use_container_width=True)

# PRIORITY TABLE
st.subheader("🎯 Daerah Prioritas Intervensi")

st.dataframe(
    priority.sort_values("rata_rata_stunting", ascending=False),
    use_container_width=True
)
