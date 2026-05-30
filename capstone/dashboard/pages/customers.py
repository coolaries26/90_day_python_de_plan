"""
customers.py — Customer LTV + Churn Risk page
Requirements:
1. Title: "👥 Customer Analytics"
2. Sidebar filter: Value Segment (All/Bronze/Silver/Gold/Platinum)
3. KPIs: Total Customers, Repeat Rate, Avg LTV, Avg Review
4. LTV histogram (px.histogram, x="total_spent", nbins=40)
5. Scatter: total_spent vs total_orders, colored by value_segment
6. Churn risk table: customers with is_churned=1 and total_spent > 200
   (these are high-value customers who churned — priority retention targets)
7. Download CSV
"""
import streamlit as st
from pathlib import Path
import sys
PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PATH))
#print(f"DEBUG: Added {PATH} to sys.path")
from an_db import load_customer_ltv
import plotly.express as px
import plotly.graph_objects as go
def render():
    st.title("👥 Customer Analytics")
    df = load_customer_ltv()
    segment = st.sidebar.selectbox("Value Segment", ["All","Bronze","Silver","Gold","Platinum"])
    if segment != "All":
        df = df[df["value_segment"] == segment]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df):,}")
    repeat_rate = (df["total_orders"] > 1).mean() * 100
    col2.metric("Repeat Rate", f"{repeat_rate:.2f}%")
    avg_ltv = df["total_spent"].mean()
    col3.metric("Avg LTV", f"${avg_ltv:.2f}")
    avg_review = df["avg_review_score"].mean()
    col4.metric("Avg Review", f"{avg_review:.2f}")
    st.markdown("---")
    fig1 = px.histogram(df, x="total_spent", nbins=40, title="Distribution of Customer LTV")
    st.plotly_chart(fig1)
    fig2 = px.scatter(df, x="total_orders", y="total_spent", color="value_segment", title="Total Spent vs Total Orders")
    st.plotly_chart(fig2)
    churn = df[df["is_churned"] == 1]
    churn = churn[churn["total_spent"] > 200]
    st.subheader("High-Value Churned Customers")
    st.table(churn.head(10))
    csv = churn.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", data=csv, file_name="high_value_churned_customers.csv")
    st.markdown("---")
