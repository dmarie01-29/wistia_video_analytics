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
    s3_staging_dir = "s3://wistia-analytics-raw-871049984307-us-east-1-an/
except Exception as e:
    st.error("🔑 Secrets configuration missing! Please ensure your credentials are set up inside the Streamlit Secrets tab.")
    st.stop()

@st.cache_data(ttl=300)  # Caches data views for 5 minutes to minimize AWS query billing scan costs
def run_athena_query(query_string):
    """Establishes an automated programmatic query connection to your Athena endpoint."""
    conn = connect(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region,
        s3_staging_dir=s3_staging_dir
    )
    return pd.read_sql(query_string, conn)

st.sidebar.header("🎛 Dashboard Navigation & Filters")
st.sidebar.markdown("Use these filters to refine your data views.")

# Query 1: Extract high-level summary KPIs
kpi_query = """
SELECT 
    COUNT(DISTINCT media_id) as total_monitored_videos,
    SUM(play_count) as global_plays,
    AVG(play_rate) * 100 as avg_play_rate_pct,
    AVG(watched_percent) as avg_completion_score
FROM wistia_analytics_db.fact_media_engagement;
"""

# Query 2: Extract detailed granular media dimensional metrics
table_query = """
SELECT 
    m.title as video_title,
    m.channel as platform_channel,
    f.date as transaction_timestamp,
    f.visitor_id,
    f.play_count,
    f.total_watch_time as watch_seconds,
    f.watched_percent
FROM wistia_analytics_db.fact_media_engagement f
JOIN wistia_analytics_db.dim_media m ON f.media_id = m.media_id
ORDER BY f.date DESC;
"""

with st.spinner("Syncing analytical views with S3 Delta Lake Warehouse..."):
    try:
        df_kpi = run_athena_query(kpi_query)
        df_metrics = run_athena_query(table_query)
    except Exception as err:
        st.error(f"❌ Connection Interrupted: {err}")
        st.stop()

# --- RENDER KPI INSIGHT MATRICES ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="📹 Monitored Asset Footprint", value=int(df_kpi["total_monitored_videos"].iloc[0]))
with col2:
    st.metric(label="▶ Total Registered Plays", value=int(df_kpi["global_plays"].iloc[0]))
with col3:
    st.metric(label="📈 Average Play Rate Score", value=f"{df_kpi['avg_play_rate_pct'].iloc[0]:.1f}%")
with col4:
    st.metric(label="⏱ Average Completion Rate", value=f"{df_kpi['avg_completion_score'].iloc[0]:.1f}%")

st.markdown("---")

# --- RENDER GRAPHICAL METRICS ---
st.subheader("📊 Engagement Trends Over Time")
if not df_metrics.empty:
    # Structure time-series parsing for interactive graph plotting
    df_metrics['transaction_timestamp'] = pd.to_datetime(df_metrics['transaction_timestamp'])
    time_chart_data = df_metrics.groupby(df_metrics['transaction_timestamp'].dt.date).size().reset_index(name='Engagement Events')
    st.line_chart(data=time_chart_data, x='transaction_timestamp', y='Engagement Events', use_container_width=True)
else:
    st.info("No time series variations recorded yet.")

# --- RENDER GRANULAR RELATIONAL AUDIT LOGS ---
st.subheader("🕵 Visitor-Level Event Audit Logs")
if not df_metrics.empty:
    st.dataframe(df_metrics, use_container_width=True, hide_index=True)
else:
    st.warning("Analytical tables are currently empty.")
    
st.sidebar.success("✅ Secure AWS Athena Connection Active")


