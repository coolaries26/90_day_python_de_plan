"""
sellers.py — Seller Performance page
Requirements:
1. Title: "🏪 Seller Performance"
2. Sidebar filter: State (multiselect from seller_state column)
3. KPIs: Total Sellers, Avg Revenue, Avg Rating, Avg On-Time Rate
4. Scatter: total_revenue vs avg_review_score, colored by on_time_delivery_rate
5. Top 10 sellers table (by total_revenue)
6. Bar chart: avg on_time_delivery_rate by seller_state
"""
import streamlit as st
from pathlib import Path
import sys
PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PATH))
print(f"DEBUG: Added {PATH} to sys.path")
from an_db import load_seller_performance
import plotly.express as px
import plotly.graph_objects as go
def render():
    st.title("🏪 Seller Performance")
    df = load_seller_performance()
    states = st.sidebar.multiselect("State", df["seller_state"].unique())
    if states:
        df = df[df["seller_state"].isin(states)]
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Sellers", f"{len(df):,}")
    col2.metric("Avg Revenue", f"${df['total_revenue'].mean():,.2f}")
    col3.metric("Avg Rating", f"{df['avg_review_score'].mean():.2f} ⭐")
    col4.metric("Avg On-Time Rate", f"{df['on_time_delivery_rate'].mean():.2f}%")
    fig1 = px.scatter(df, x="total_revenue", y="avg_review_score", color="on_time_delivery_rate", title="Revenue vs Review Score colored by On-Time Rate")
    st.plotly_chart(fig1)
    top10 = df.head(10)    
    st.subheader("Top 10 Sellers by Revenue")
    st.table(top10[["seller_id", "seller_state", "total_revenue", "avg_review_score", "on_time_delivery_rate"]])
    on_time_by_state = df.groupby("seller_state")["on_time_delivery_rate"].mean().reset_index()
    fig2 = px.bar(on_time_by_state, x="seller_state", y="on_time_delivery_rate", title="Avg On-Time Delivery Rate by Seller State")
    st.plotly_chart(fig2)
    st.markdown("---")