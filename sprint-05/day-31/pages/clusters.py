"""
clusters.py — Customer Clusters Streamlit Page
===============================================
YOUR TASK: Build the customer clusters page.

Requirements:
1. Page title: "🔵 Customer Clusters"
2. Load cluster data:
   cluster_df = pd.read_sql(
       "SELECT c.*, a.total_spend, a.total_rentals, a.value_segment
        FROM analytics_customer_clusters c
        JOIN analytics_customer_airflow a USING (customer_id)",
       engine
   )
3. Show metric: Number of clusters (cluster_df["cluster"].nunique())
4. Show cluster size bar chart:
   px.bar(cluster_df.groupby("cluster").size().reset_index(name="count"),
          x="cluster", y="count", title="Customers per Cluster")
5. Show scatter: total_spend vs total_rentals, colored by cluster
6. Show cluster profiles table (load from cluster_profiles.csv)
7. Add to app.py sidebar as "🔵 Clusters"

HINTS for loading profile CSV:
  profile_path = Path(__file__).resolve().parent.parent.parent
      / "sprint-06" / "day-41" / "output" / "cluster_profiles.csv"
  if profile_path.exists():
      st.dataframe(pd.read_csv(profile_path))
"""
# YOUR CODE HERE
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent / "sprint-01" / "day-04"))    # for logger
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent / "sprint-01" / "day-02"))    # for db_utils
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent / "sprint-06" / "day-41"))    # for profile_clusters
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent / "sprint-05" / "day-31"))    # for load_customer_features

from kmeans_segmentation import profile_clusters
from logger import get_pipeline_logger
from db_utils import get_engine, dispose_engine
logger = get_pipeline_logger(__name__)
engine = get_engine()
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "sprint-06" / "day-41" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def render():
    st.title("🔵 Customer Clusters")
    st.markdown("---")
    # YOUR CODE HERE
    cluster_df = pd.read_sql(
        """SELECT c.*, a.total_spend, a.total_rentals, a.value_segment 
        FROM analytics_customer_clusters c JOIN analytics_customer_airflow a USING (customer_id)""",
        engine
    )
    st.metric("Number of clusters", cluster_df["cluster"].nunique())
    cluster_counts = cluster_df.groupby("cluster").size().reset_index(name="count")
    fig_bar = px.bar(cluster_counts, x="cluster", y="count", title="Customers per Cluster")
    st.plotly_chart(fig_bar)
    fig_scatter = px.scatter(cluster_df, x="total_spend", y="total_rentals", color="cluster",
                             title="Total Spend vs Total Rentals by Cluster")
    st.plotly_chart(fig_scatter)
    profile_path = OUTPUT_DIR / "cluster_profiles.csv"
    if profile_path.exists():
        profile_df = pd.read_csv(profile_path)
        st.subheader("Cluster Profiles")
        st.dataframe(profile_df)
    