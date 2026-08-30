"""
app_wistia_dashboard.py

Streamlit dashboard for the marketing team, reading the gold-tier Delta tables
produced by the Wistia medallion pipeline (dim_media, dim_visitor, fact_media_engagement).

Deploy via Streamlit Community Cloud, pulling from GitHub — same pattern as app_gp.py.
AWS credentials are read from st.secrets, not local disk.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from deltalake import DeltaTable

st.set_page_config(page_title="Wistia Video Engagement Dashboard", layout="wide")

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
BUCKET_NAME = st.secrets.get("BUCKET_NAME", "")
AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = st.secrets.get("AWS_REGION", "us-east-1")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": AWS_REGION,
}

GOLD_DIM_MEDIA_PATH = f"s3://{BUCKET_NAME}/gold/dim_media"
GOLD_DIM_VISITOR_PATH = f"s3://{BUCKET_NAME}/gold/dim_visitor"
GOLD_FACT_ENGAGEMENT_PATH = f"s3://{BUCKET_NAME}/gold/fact_media_engagement"


# ----------------------------------------------------
# DATA LOADING (cached so repeat interactions don't re-hit S3)
# ----------------------------------------------------
@st.cache_data(ttl=600)
def load_gold_tables():
    dim_media = DeltaTable(GOLD_DIM_MEDIA_PATH, storage_options=STORAGE_OPTIONS).to_pandas()
    dim_visitor = DeltaTable(GOLD_DIM_VISITOR_PATH, storage_options=STORAGE_OPTIONS).to_pandas()
    fact = DeltaTable(GOLD_FACT_ENGAGEMENT_PATH, storage_options=STORAGE_OPTIONS).to_pandas()

    fact["date"] = pd.to_datetime(fact["date"], errors="coerce")

    enriched = (
        fact.merge(dim_media, on="media_id", how="left", suffixes=("", "_media"))
            .merge(dim_visitor, on="visitor_id", how="left", suffixes=("", "_visitor"))
    )
    return dim_media, dim_visitor, fact, enriched


try:
    dim_media, dim_visitor, fact, df = load_gold_tables()
except Exception as e:
    st.error(f"Could not load data from S3: {e}")
    st.stop()

if df.empty:
    st.warning("No engagement data available yet. Check that the pipeline has run successfully.")
    st.stop()

# ----------------------------------------------------
# SIDEBAR FILTERS
# ----------------------------------------------------
st.sidebar.header("Filters")

min_date, max_date = df["date"].min(), df["date"].max()
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date.date(), max_date.date()) if pd.notna(min_date) else None,
)

video_options = ["All videos"] + sorted(df["title"].dropna().unique().tolist())
selected_video = st.sidebar.selectbox("Video", video_options)

country_options = ["All countries"] + sorted(df["country"].dropna().unique().tolist())
selected_country = st.sidebar.selectbox("Visitor country", country_options)

filtered = df.copy()
if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered = filtered[(filtered["date"] >= start) & (filtered["date"] <= end)]
if selected_video != "All videos":
    filtered = filtered[filtered["title"] == selected_video]
if selected_country != "All countries":
    filtered = filtered[filtered["country"] == selected_country]

# ----------------------------------------------------
# HEADER + KPI ROW
# ----------------------------------------------------
st.title("Wistia Video Engagement Dashboard")
st.caption("Media-level and visitor-level analytics from the Wistia Stats API pipeline.")

total_plays = filtered["play_count"].sum()
unique_visitors = filtered["visitor_id"].nunique()
avg_watched_pct = filtered["watched_percent"].mean()
active_videos = filtered["media_id"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Plays", f"{total_plays:,.0f}")
k2.metric("Unique Visitors", f"{unique_visitors:,}")
k3.metric("Avg. % Watched", f"{avg_watched_pct:.1f}%" if pd.notna(avg_watched_pct) else "—")
k4.metric("Active Videos", f"{active_videos:,}")

st.divider()

# ----------------------------------------------------
# TABS
# ----------------------------------------------------
tab_overview, tab_videos, tab_visitors, tab_raw = st.tabs(
    ["Trends", "Video Performance", "Visitor Insights", "Raw Data"]
)

# --- Trends tab ---
with tab_overview:
    st.subheader("Engagement Over Time")

    if filtered["date"].notna().any():
        daily = (
            filtered.dropna(subset=["date"])
            .assign(day=filtered["date"].dt.date)
            .groupby("day")
            .agg(plays=("play_count", "sum"), unique_visitors=("visitor_id", "nunique"))
            .reset_index()
        )
        fig_trend = px.line(
            daily, x="day", y=["plays", "unique_visitors"],
            labels={"value": "Count", "day": "Date", "variable": "Metric"},
            title="Daily Plays vs. Unique Visitors",
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No dated events in the current filter selection.")

    st.subheader("Watch-Percentage Distribution")
    fig_hist = px.histogram(
        filtered, x="watched_percent", nbins=20,
        labels={"watched_percent": "% of Video Watched"},
        title="How Much of the Video Do Viewers Typically Watch?",
    )
    st.plotly_chart(fig_hist, use_container_width=True)

# --- Video Performance tab ---
with tab_videos:
    st.subheader("Top Videos by Plays")

    video_perf = (
        filtered.groupby("title")
        .agg(
            plays=("play_count", "sum"),
            unique_visitors=("visitor_id", "nunique"),
            avg_watched_pct=("watched_percent", "mean"),
        )
        .reset_index()
        .sort_values("plays", ascending=False)
    )

    fig_bar = px.bar(
        video_perf, x="title", y="plays",
        title="Plays by Video",
        labels={"title": "Video", "plays": "Plays"},
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Engagement Quality by Video")
    fig_scatter = px.scatter(
        video_perf, x="plays", y="avg_watched_pct", size="unique_visitors",
        hover_name="title",
        labels={"plays": "Total Plays", "avg_watched_pct": "Avg. % Watched"},
        title="Reach (plays) vs. Engagement Depth (avg. % watched)",
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    st.dataframe(video_perf, use_container_width=True)

# --- Visitor Insights tab ---
with tab_visitors:
    st.subheader("Visitors by Country")

    country_counts = (
        filtered.groupby("country")["visitor_id"]
        .nunique()
        .reset_index(name="unique_visitors")
        .sort_values("unique_visitors", ascending=False)
    )

    fig_country = px.bar(
        country_counts.head(20), x="country", y="unique_visitors",
        title="Top 20 Countries by Unique Visitors",
    )
    st.plotly_chart(fig_country, use_container_width=True)

    st.subheader("Returning vs. One-Time Viewers")
    visits_per_visitor = filtered.groupby("visitor_id")["play_count"].sum()
    returning = (visits_per_visitor > 1).sum()
    one_time = (visits_per_visitor == 1).sum()
    fig_pie = px.pie(
        names=["Returning viewers", "One-time viewers"],
        values=[returning, one_time],
        title="Viewer Loyalty Split",
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# --- Raw Data tab ---
with tab_raw:
    st.subheader("Filtered Engagement Records")
    st.dataframe(filtered, use_container_width=True)
    st.download_button(
        "Download filtered data as CSV",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="wistia_engagement_filtered.csv",
        mime="text/csv",
    )
