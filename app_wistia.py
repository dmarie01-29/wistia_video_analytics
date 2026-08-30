import streamlit as st
import pandas as pd
from pyathena import connect

# Set up page configurations
st.set_page_config(
    page_title="Wistia Video Analytics Control Center",
    page_icon="🧠",
    layout="wide"
)

# Application Title Block
st.title("🧠 Wistia Business Video Analytics Control Center")
st.markdown("Real-time pipeline data pulled serverless from **AWS S3 Delta Lake** using **Amazon Athena** query routing.")

# Retrieve environment connection parameters from Streamlit's encrypted Secrets Vault
try:
    aws_access_key = st.secrets["aws_access_key_id"]
    aws_secret_key = st.secrets["aws_secret_access_key"]
    # aws_region = st.secrets["aws_region"]
    # s3_staging_dir = st.secrets["s3_staging_dir"]
    aws_region = "us-east-1"
    s3_staging_dir = "s3://wistia-analytics-raw-871049984307-us-east-1-an/"
except Exception as e:
    st.error("🔑 Secrets configuration missing! Verify your Streamlit Secrets tab parameters.")
    st.stop()

@st.cache_data(ttl=60)  # Short caching execution logic for responsive dashboard updates
def run_athena_query(query_string):
    conn = connect(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
        s3_staging_dir=s3_staging_dir
    )
    return pd.read_sql(query_string, conn)

# -------------------------------------------------------------------
# REQUIREMENT TARGETING: FR3 & FR4 - Media-Level Metadata & Metrics
# -------------------------------------------------------------------
media_metrics_query = """
SELECT 
    m.media_id AS "Video ID",
    m.title AS "Video Title",
    m.channel AS "Distribution Channel",
    m.created_at AS "Ingested Timestamp",
    SUM(f.play_count) AS "Total Plays (FR4)",
    AVG(f.play_rate) * 100 AS "Average Play Rate % (FR4)",
    SUM(f.total_watch_time) AS "Total Watch Time (Sec) (FR4)"
FROM wistia_analytics_db.fact_media_engagement f
JOIN wistia_analytics_db.dim_media m ON f.media_id = m.media_id
GROUP BY m.media_id, m.title, m.channel, m.created_at;
"""

# -------------------------------------------------------------------
# REQUIREMENT TARGETING: FR5 - Visitor-Level Data & Engagement Events
# -------------------------------------------------------------------
visitor_events_query = """
SELECT 
    f.date AS "Event Date/Time",
    f.visitor_id AS "Visitor ID (FR5)",
    v.ip_address AS "IP Address (FR5)",
    v.country AS "Country",
    m.title AS "Interacted Video",
    f.watched_percent AS "Completion % (FR5)"
FROM wistia_analytics_db.fact_media_engagement f
JOIN wistia_analytics_db.dim_visitor v ON f.visitor_id = v.visitor_id
JOIN wistia_analytics_db.dim_media m ON f.media_id = m.media_id
ORDER BY f.date DESC;
"""

with st.spinner("Streaming operational matrices from S3 Gold Delta Tables..."):
    try:
        df_media = run_athena_query(media_metrics_query)
        df_visitor = run_athena_query(visitor_events_query)
    except Exception as err:
        st.error(f"Athena Execution Error: {err}")
        st.stop()

# --- TAB VIEW RENDERING FOR CLEAN ASSIGNMENT GRADING ---
tab1, tab2 = st.tabs(["📹 Media-Level Performance (FR3 & FR4)", "🕵 Visitor-Level Tracking (FR5)"])

with tab1:
    st.subheader("Extracting Video Metadata & Engagement Summaries")
    st.markdown("Provides the marketing team visibility into overall asset penetration across channels.")
    
    if not df_media.empty:
        # Highlight top aggregate performance scores using metrics indicators
        m_col1, m_col2, m_col3 = st.columns(3)
        m_col1.metric("Global Ingested Views", value=int(df_media["Total Plays (FR4)"].sum()))
        m_col2.metric("Mean Performance Play Rate", value=f"{df_media['Average Play Rate % (FR4)'].mean():.1f}%")
        m_col3.metric("Accumulated Retention (Sec)", value=int(df_media["Total Watch Time (Sec) (FR4)"].sum()))
        
        st.dataframe(df_media, use_container_width=True, hide_index=True)
    else:
        st.info("No media analytics records resolved.")

with tab2:
    st.subheader("Auditing Granular Visitor Playback Sessions")
    st.markdown("Maps distinct user footprint signals (IPs, locations, watch behaviors) to verify unique interactions.")
    
    if not df_visitor.empty:
        st.dataframe(df_visitor, use_container_width=True, hide_index=True)
        
        # Simple distribution metrics overview for marketing strategy analysis
        st.subheader("🌍 Geographic Engagement Footprint")
        geo_data = df_visitor.groupby("Country").size().reset_index(name="Sessions Logged")
        st.bar_chart(data=geo_data, x="Country", y="Sessions Logged", use_container_width=True)
    else:
        st.info("No visitor metrics records resolved.")
