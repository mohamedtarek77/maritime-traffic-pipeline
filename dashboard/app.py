"""
app.py
-------
لوحة تحكم Streamlit تعرض:
    1) خريطة حية لمواقع السفن آخر بيانات Silver
    2) جدول ازدحام الموانئ من طبقة Gold (عدد السفن، مدة البقاء)
"""

import glob
import os

import pandas as pd
import streamlit as st

SILVER_PATH = "/app/data/silver/ais_positions"
GOLD_PATH = "/app/data/gold/port_congestion"

st.set_page_config(page_title="Maritime Traffic Intelligence", layout="wide")
st.title("🚢 لوحة تحكم حركة السفن في موانئ الإمارات")
st.caption("مصدر البيانات: AIS عبر aisstream.io — معالجة عبر Kafka + Spark")


def load_latest_parquet(base_path: str) -> pd.DataFrame:
    """يقرأ كل ملفات Parquet المتوفرة تحت المسار المعطى ويجمعها في DataFrame واحد."""
    files = glob.glob(os.path.join(base_path, "**", "*.parquet"), recursive=True)
    if not files:
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)


col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("خريطة مواقع السفن الحالية")
    positions_df = load_latest_parquet(SILVER_PATH)
    if positions_df.empty:
        st.info("لا توجد بيانات بعد. تأكد من تشغيل خط الاستيعاب ومهمة Bronze→Silver.")
    else:
        latest_positions = (
            positions_df.sort_values("processed_at")
            .groupby("mmsi")
            .tail(1)
        )
        st.map(
            latest_positions.rename(columns={"latitude": "lat", "longitude": "lon"})[
                ["lat", "lon"]
            ]
        )
        st.caption(f"عدد السفن المعروضة: {latest_positions['mmsi'].nunique()}")

with col2:
    st.subheader("ازدحام الموانئ (آخر تقرير)")
    congestion_df = load_latest_parquet(GOLD_PATH)
    if congestion_df.empty:
        st.info("لا يوجد تقرير Gold بعد. شغّل مهمة silver_to_gold_port_congestion.py.")
    else:
        latest_report_date = congestion_df["report_date"].max()
        latest_congestion = congestion_df[congestion_df["report_date"] == latest_report_date]
        st.dataframe(
            latest_congestion[
                ["port_name", "vessel_count", "min_dwell_hours", "max_dwell_hours"]
            ].sort_values("vessel_count", ascending=False),
            use_container_width=True,
        )

st.divider()
st.caption("مشروع بورتفوليو: Maritime Traffic Intelligence Pipeline — Kafka · Spark · Airflow · MinIO · Streamlit")
