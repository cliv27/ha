import streamlit as st
import pandas as pd
import subprocess
import tempfile
import os

# ======================================================
# PAGE CONFIG
# ======================================================
st.set_page_config(
    page_title="Dashboard Stunting Indonesia",
    layout="wide"
)

# ======================================================
# UTIL FUNCTION
# ======================================================
def load_parquet_from_hdfs(hdfs_path: str) -> pd.DataFrame:
    """
    Download parquet directory from HDFS to temp folder
    and load into Pandas DataFrame.
    """
    tmp_dir = tempfile.mkdtemp()
    subprocess.run(
        ["hdfs", "dfs", "-get", hdfs_path, tmp_dir],
        check=True
    )

    parquet_files = []
    for root, _, files in os.walk(tmp_dir):
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))

    if not parquet_files:
        raise ValueError(f"No parquet files found in {hdfs_path}")

    return pd.read_parquet(parquet_files)


def detect_value_column(df: pd.DataFrame) -> str:
    """
    Automatically detect numeric prevalence column.
    """
    for col in df.columns:
        if df[col].dtype in ["float64", "int64"]:
            return col
    raise ValueError("No numeric column detected for analysis")


# ======================================================
# LOAD DATA (FROM HDFS)
# ======================================================
priority = load_parquet_from_hdfs(
    "/project/stunting/analytics/priority_regions"
)
trend = load_parquet_from_hdfs(
    "/project/stunting/analytics/national_trend"
)
problem = load_parquet_from_hdfs(
    "/project/stunting/analytics/problem_regions"
)

# ======================================================
# NORMALIZE & VALIDATE SCHEMA
# ======================================================
priority_value_col = detect_value_column(priority)
trend_value_col = detect_value_column(trend)

# Pastikan kolom utama ada
assert "kabupaten_kota" in priority.columns
assert "tahun" in trend.columns
assert "delta" in problem.columns

# ======================================================
# HEADER
# ======================================================
st.title("📊 Analisis Big Data Stunting Indonesia")
st.caption(
    "Dashboard analisis kesehatan publik berbasis Hadoop (HDFS) "
    "dan Spark untuk mendukung pengambilan keputusan kebijakan."
)

st.divider()

# ======================================================
# KPI SECTION (RINGKAS, TAPI BERMAKNA)
# ======================================================
avg_national = round(trend[trend_value_col].mean(), 2)
worst_region = priority.iloc[0]["kabupaten_kota"]
worst_value = round(priority.iloc[0][priority_value_col], 2)
problem_regions = problem[problem["delta"] > 0]["kabupaten_kota"].nunique()

k1, k2, k3 = st.columns(3)

k1.metric(
    "📉 Rata-rata Nasional",
    f"{avg_national} %",
    help="Rata-rata prevalensi stunting nasional (2021–2023)"
)

k2.metric(
    "🚨 Wilayah Terparah",
    worst_region,
    f"{worst_value} %",
)

k3.metric(
    "⚠️ Wilayah Memburuk",
    f"{problem_regions} daerah",
    help="Daerah dengan tren peningkatan prevalensi"
)

st.divider()

# ======================================================
# ROW 1 — PRIORITAS & TREN
# ======================================================
left, right = st.columns([1.3, 1])

with left:
    st.subheader("🚨 Daerah Prioritas Intervensi")
    st.write(
        "Wilayah dengan **rata-rata prevalensi stunting tertinggi** "
        "selama 2021–2023."
    )

    st.dataframe(
        priority[["kabupaten_kota", priority_value_col]]
        .rename(columns={priority_value_col: "rata_rata_stunting"})
        .head(10),
        use_container_width=True,
        height=340
    )

with right:
    st.subheader("📈 Tren Nasional Stunting")

    trend_sorted = trend.sort_values("tahun")

    st.line_chart(
        trend_sorted.set_index("tahun")[trend_value_col],
        use_container_width=True
    )

    st.caption(
        "Grafik ini menunjukkan apakah kebijakan nasional "
        "berhasil menurunkan stunting dari tahun ke tahun."
    )

st.divider()

# ======================================================
# ROW 2 — MASALAH KEBIJAKAN
# ======================================================
left2, right2 = st.columns([1, 1.3])

with left2:
    st.subheader("⚠️ Daerah dengan Tren Memburuk")
    st.write(
        "Wilayah yang mengalami **kenaikan prevalensi** "
        "dibanding tahun sebelumnya."
    )

    st.dataframe(
        problem.sort_values("delta", ascending=False)
        .head(10)[["kabupaten_kota", "tahun", "delta"]],
        use_container_width=True,
        height=340
    )

with right2:
    st.subheader("🏆 5 Wilayah Terburuk Nasional")

    top5 = (
        priority
        .head(5)
        .set_index("kabupaten_kota")[priority_value_col]
    )

    st.bar_chart(
        top5,
        use_container_width=True
    )

st.divider()

# ======================================================
# INSIGHT & DECISION SUPPORT
# ======================================================
st.subheader("🧠 Insight & Rekomendasi Kebijakan")

st.markdown("""
### Temuan Utama
- Prevalensi stunting **tidak merata** dan terkonsentrasi pada wilayah tertentu.
- Beberapa daerah menunjukkan **tren memburuk**, menandakan kegagalan intervensi.
- Penurunan nasional berlangsung **lambat**, membutuhkan kebijakan berbasis wilayah.

### Rekomendasi Pengambilan Keputusan
1. **Prioritaskan anggaran dan program gizi** pada wilayah dengan prevalensi tertinggi.
2. **Audit kebijakan kesehatan** di daerah dengan tren peningkatan stunting.
3. Terapkan **monitoring berbasis data tahunan** untuk evaluasi program.

Dashboard ini dirancang untuk mendukung **data-driven policy making**.
""")

st.success(
    "Kesimpulan: Analisis Big Data memungkinkan penentuan kebijakan "
    "yang lebih tepat sasaran dan terukur."
)
