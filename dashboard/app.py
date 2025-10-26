from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# Optional mapping libs (install if using Folium heat map)
import folium
from streamlit_folium import st_folium
from folium.plugins import HeatMap

DB_PATH = Path("data/db/milwaukee_crashes.db")

st.set_page_config(page_title="Milwaukee Crash Insights", layout="wide")
st.title("🚦 Milwaukee Crash Insights")
st.caption("Source: trafficaccident.csv → SQLite → Streamlit")

# Data loading
@st.cache_data(ttl=300)
def load_data(limit=None):
    if not DB_PATH.exists():
        st.error("Database not found at data/db/milwaukee_crashes.db. Run the load step first.")
        st.stop()

    engine = create_engine(f"sqlite:///{DB_PATH}")
    query = "SELECT * FROM crashes"
    if limit and limit > 0:
        query += f" LIMIT {int(limit)}"

    df = pd.read_sql(query, engine, parse_dates=["crash_datetime"])
    # dtypes cleanup
    if "is_weekend" in df.columns:
        df["is_weekend"] = pd.to_numeric(df["is_weekend"], errors="coerce").fillna(0).astype(int)
    if "hour_of_day" in df.columns:
        df["hour_of_day"] = pd.to_numeric(df["hour_of_day"], errors="coerce")
    return df

with st.sidebar:
    st.header("Filters")
    # Cache clear button
    if st.button("Clear data cache"):
        st.cache_data.clear()
        st.experimental_rerun()

    sample_n = st.number_input("Row sample (optional)", min_value=0, value=0, step=1000)
    df = load_data(sample_n if sample_n > 0 else None)

    # Date range
    min_dt = pd.to_datetime(df["crash_datetime"].min())
    max_dt = pd.to_datetime(df["crash_datetime"].max())
    date_range = st.date_input("Date range", (min_dt.date(), max_dt.date()))
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start = pd.to_datetime(date_range[0])
        end = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)  # inclusive end
        df = df[(df["crash_datetime"] >= start) & (df["crash_datetime"] < end)]

    # Location keyword
    kw = st.text_input("Location keyword (e.g., 'HOWELL', '27TH')")
    if kw:
        df = df[df["crash_location"].str.contains(kw.upper(), na=False)]

    # Weekend/weekday toggle
    ww = st.selectbox("Day type", ["All days", "Weekdays only", "Weekends only"])
    if ww == "Weekdays only":
        df = df[df["is_weekend"] == 0]
    elif ww == "Weekends only":
        df = df[df["is_weekend"] == 1]

    # Download current view
    st.download_button(
        "Download current view (CSV)",
        df.to_csv(index=False).encode("utf-8"),
        file_name="milwaukee_crashes_filtered.csv",
        mime="text/csv",
    )

# KPIs 
col1, col2, col3 = st.columns(3)
col1.metric("Total crashes", f"{len(df):,}")

if "hour_of_day" in df and df["hour_of_day"].notna().any():
    col2.metric("Avg hour of day", f"{df['hour_of_day'].dropna().mean():.1f}")
else:
    col2.metric("Avg hour of day", "—")

if "is_weekend" in df and len(df):
    wknd_pct = 100 * df["is_weekend"].mean()
    col3.metric("Weekend share", f"{wknd_pct:.1f}%")
else:
    col3.metric("Weekend share", "—")

st.divider()

# Charts 
weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

st.subheader("Crashes by Day of Week")
if "day_of_week" in df.columns and df["day_of_week"].notna().any():
    by_dow = (
        df["day_of_week"].value_counts()
        .reindex(weekday_order)
        .fillna(0)
        .astype(int)
        .rename("count")
        .to_frame()
    )
    st.bar_chart(by_dow)
else:
    st.info("No day_of_week column available.")

st.subheader("Crashes by Month")
if "month" in df.columns and df["month"].notna().any():
    by_month = (
        df["month"].value_counts().sort_index()
        .rename("count").to_frame()
        .assign(month_name=lambda x: x.index.map(lambda m: pd.Timestamp(2000, int(m), 1).strftime("%b")))
        .set_index("month_name")
    )
    st.bar_chart(by_month[["count"]])
else:
    st.info("No month column available.")

# Heat map
st.subheader("Crash Heat Map")
if {"lat", "lon"}.issubset(df.columns):
    geo = df[["lat", "lon"]].dropna()
    if geo.empty:
        st.info("No geocoded points to display yet.")
    else:
        center = [geo["lat"].mean(), geo["lon"].mean()]
        m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")
        HeatMap(geo.values.tolist(), radius=12, blur=15, max_zoom=13, min_opacity=0.3).add_to(m)
        st_folium(m, width=None, height=520)
else:
    st.info("No 'lat'/'lon' columns found. Re-run the load step after geocoding.")

st.subheader("Recent Records")
preview_cols = [c for c in ["case_number","crash_datetime","crash_location","lat","lon"] if c in df.columns]
st.dataframe(
    df.sort_values("crash_datetime", ascending=False)[preview_cols].head(50),
    width='stretch'
)
