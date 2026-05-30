"""
orders.py — Orders + Delivery Analysis page
YOUR TASK: Build the orders page.

Requirements:
1. Title: "📦 Order & Delivery Analytics"
2. Sidebar filter: Delivery Status (All / On Time / Late)
3. 3 KPI metrics:
   - Total Orders (filtered)
   - Late Delivery Rate (%)
   - Avg Delivery Days
4. Key insight box (st.info or st.warning):
   "⚡ Late orders receive avg X.XX ⭐ vs Y.YY ⭐ for on-time orders"
   (Use the 4.29 vs 2.57 finding from Day 44)
5. Delivery days histogram (px.histogram, x="delivery_days_actual", nbins=30)
6. Review score by is_late (px.box, x="is_late", y="review_score")
7. CSV download button

HINTS:
  from db import load_order_metrics
  df = load_order_metrics()

  # Sidebar filter
  with st.sidebar:
      st.markdown("---")
      status = st.selectbox("Delivery Status", ["All","On Time","Late"])

  filtered = df.copy()
  if status == "On Time":  filtered = df[df["is_late"] == 0]
  if status == "Late":     filtered = df[df["is_late"] == 1]
"""
import streamlit as st
from pathlib import Path
import sys
PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PATH))
#print(f"DEBUG: Added {PATH} to sys.path")
from an_db import load_order_metrics
import plotly.express as px
import plotly.graph_objects as go
def render():
    st.title("📦 Order & Delivery Analytics")
    with st.sidebar:
        st.markdown("---")
        status = st.selectbox("Delivery Status", ["All","On Time","Late"])
    df = load_order_metrics()
    filtered = df.copy()
    if status == "On Time":  filtered = df[df["is_late"] == 0]
    if status == "Late":     filtered = df[df["is_late"] == 1]
    col1, col2, col3 = st.columns(3)
   
    total_orders = len(filtered)
    late_rate = filtered["is_late"].mean() * 100
    avg_delivery_days = filtered["delivery_days_actual"].mean()
    col1.metric("Total Orders", f"{total_orders:,}")
    col2.metric("Late Delivery Rate", f"{late_rate:.2f}%")
    col3.metric("Avg Delivery Days", f"{avg_delivery_days:.2f}")
    st.markdown("---")
    on_time_avg = df[df["is_late"] == 0]["review_score"].mean()
    late_avg = df[df["is_late"] == 1]["review_score"].mean()
    st.info(f"⚡ Late orders receive avg {late_avg:.2f} ⭐ vs {on_time_avg:.2f} ⭐ for on-time orders")
    fig1 = px.histogram(filtered, x="delivery_days_actual", nbins=30, title="Distribution of Actual Delivery Days", color_discrete_map={"0": "seagreen", "1": "tomato"}, labels={"delivery_days_actual": "Actual Delivery Days"})
    st.plotly_chart(fig1)
    fig2 = px.box(filtered, x="is_late", y="review_score", title="Review Score by Delivery Status", labels={"is_late": "Is Late"}, color_discrete_sequence=["seagreen", "tomato"])
    st.plotly_chart(fig2)
    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="filtered_orders.csv",
        mime="text/csv"
    )
    st.markdown("---")
