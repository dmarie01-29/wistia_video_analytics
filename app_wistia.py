"""
app_wistia_dashboard.py
Streamlit dashboard for the marketing team, reading the gold-tier Delta tables 
produced by the Wistia medallion pipeline (dim_media, dim_visitor, fact_media_engagement).
Securely routes queries using serverless Amazon Athena.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from pyathena import connect

# ----------------------------------------------------
# PAGE CONFIG
# ----------------------------------------------------
st.set_page_config(
    page_title="Wistia Marketing Video Analytics",
    page_icon="🧠",
    layout="wide"
)

st.title("🧠 Wistia Cross-Channel Video Analytics Dashboard")
st.markdown("### 📊 Marketing Performance Control Center (Requirements Validation)")

# ----------------------------------------------------
# CONFIG — retrieve environment connection parameters from Streamlit's Secrets Vault
# ----------------------------------------------------
try:
    aws_access_key = st.secrets["aws_access_key_id"]
    aws_secret_key = st.secrets["aws_secret_access_key"]
    aws_region = "us-east-1"
    s3_staging_dir = "s3://wistia-analytics-raw-871049984307-us-east-1-an/athena-queries/"
except Exception as e:
    st.error("🔑 Secrets configuration missing! Verify your Streamlit Secrets tab parameters.")
    st.stop()

# ----------------------------------------------------
# DATA LOADING (Fixed to route via serverless Athena client engine)
# ----------------------------------------------------
@st.cache_data(ttl=60)
def load_gold_tables():
    # Establish a reliable relational routing handshake with your AWS Catalog
    conn = connect(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
        s3_staging_dir=s3_staging_dir
    )
    
    # Read core components cleanly via ANSI SQL
    dim_media = pd.read_sql("SELECT * FROM wistia_analytics_db.dim_media", conn)
    dim_visitor = pd.read_sql("SELECT * FROM wistia_analytics_db.dim_visitor", conn)
    fact = pd.read_sql("SELECT * FROM wistia_analytics_db.fact_media_engagement", conn)
    
    # Structure timestamp tracking boundaries safely
    fact["date"] = pd.to_datetime(fact["date"], errors="coerce")
    
    # Merge dimensional data frames to enrich the central performance tracking view
    enriched = (
        fact.merge(dim_media, on="media_id", how="left", suffixes=("", "_media"))
            .merge(dim_visitor, on="visitor_id", how="left", suffixes=("", "_visitor"))
    )
    return dim_media, dim_visitor, fact, enriched

try:
    dim_media, dim_visitor, fact, df = load_gold_tables()
except Exception as e:
    st.error(f"Could not load data from S3 via Athena: {e}")
    st.stop()

if df.empty:
    st.warning("No engagement data available yet. Check that the pipeline has run successfully.")
    st.stop()

# ----------------------------------------------------
# SIDEBAR FILTERS (Preserved intact)
# ----------------------------------------------------
st.sidebar.header("Filters")
min_date, max_date = df["date"].min(), df["date"].max()

# Protect filter calculations if dates land unpopulated
if pd.notna(min_date) and pd.notna(max_date):
    date_range = st.sidebar.date_input("Date range", value=(min_date.date(), max_date.date()))
else:
    date_range = st.sidebar.date_input("Date range", value=None)

video_options = ["All videos"] + sorted(df["title"].dropna().unique().tolist())
selected_video = st.sidebar.selectbox("Video", video_options)

country_options = ["All countries"] + sorted(df["country"].dropna().unique().tolist())
selected_country = st.sidebar.selectbox("Visitor country", country_options)

filtered = df.copy()
if isinstance(date_range, tuple) and len(date_range) == 2:
    # 1. Normalize data column cleanly to a string date layout (YYYY-MM-DD)
    filtered_dates = pd.to_datetime(filtered["date"], errors="coerce").dt.date
    
    # 2. Extract boundary date variables straight from the Streamlit input tuple
    start_val = date_range[0]
    end_val = date_range[1]
    
    # 3. Apply comparison masking safely across exact matching dates
    filtered = filtered[(filtered_dates >= start_val) & (filtered_dates <= end_val)]


if selected_video != "All videos":
    filtered = filtered[filtered["title"] == selected_video]

if selected_country != "All countries":
    filtered = filtered[filtered["country"] == selected_country]

# ----------------------------------------------------
# KPI ROW (Preserved intact)
# ----------------------------------------------------
total_plays = filtered["play_count"].sum()
unique_visitors = filtered["visitor_id"].nunique()
avg_watched_pct = filtered["watched_percent"].mean()
active_videos = filtered["media_id"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Plays (FR4)", f"{total_plays:,.0f}")
k2.metric("Unique Visitors (FR5)", f"{unique_visitors:,}")
k3.metric("Avg. % Watched (FR4)", f"{avg_watched_pct:.1f}%" if pd.notna(avg_watched_pct) else "—")
k4.metric("Active Videos (FR3)", f"{active_videos:,}")

st.divider()

# ----------------------------------------------------
# VISUALIZATION TABS (Preserved intact with upgraded styling layout checks)
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
    if not filtered.empty:
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
            video_perf, x="title", y="plays", title="Plays by Video",
            labels={"title": "Video", "plays": "Plays"},
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.subheader("Engagement Quality by Video")
        fig_scatter = px.scatter(
            video_perf, x="plays", y="avg_watched_pct", size="unique_visitors",
            hover_name="title", labels={"plays": "Total Plays", "avg_watched_pct": "Avg. % Watched"},
            title="Reach (plays) vs. Engagement Depth (avg. % watched)",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.dataframe(video_perf, use_container_width=True, hide_index=True)
    else:
        st.info("No video records match selected filters.")

# --- Visitor Insights tab ---
with tab_visitors:
    st.subheader("Visitors by Country")
    if not filtered.empty:
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
        
        # Guard against rendering completely unpopulated pie plots
        if returning + one_time > 0:
            fig_pie = px.pie(
                names=["Returning viewers", "One-time viewers"],
                values=[returning, one_time], title="Viewer Loyalty Split",
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Insufficient visitor logs to compute returning profile distribution split.")
    else:
        st.info("No visitor records match selected filters.")

# --- Raw Data tab ---
with tab_raw:
    st.subheader("Filtered Engagement Records")
    st.dataframe(filtered, use_container_width=True, hide_index=True)
    st.download_button(
        "Download filtered data as CSV",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="wistia_engagement_filtered.csv",
        mime="text/csv",
    )
