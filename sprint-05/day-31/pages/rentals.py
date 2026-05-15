"""
**Requirements:**
1. Page title: "📅 Rental Analytics"
2. Load rental data via a new `load_rental_stats()` function in `db.py`:
   ```sql
   SELECT DATE_TRUNC('week', rental_date)::date AS week_start,
          COUNT(*) AS rental_count,
          COUNT(DISTINCT customer_id) AS unique_customers,
          COUNT(CASE WHEN return_date IS NULL THEN 1 END) AS still_open
   FROM rental
   GROUP BY DATE_TRUNC('week', rental_date)
   ORDER BY week_start
   ```
3. Show 3 KPI metrics: Total Rentals, Open Rentals, Unique Customers
4. Show weekly rental trend as a Plotly line chart
5. Add page to `app.py` sidebar navigation
HINTS:
Brief: On the new Rentals page, add an interactive Plotly chart that shows the weekly rental trend with:

Hoverable data points showing week_start + rental_count + unique_customers
A filled area under the line (fill="tozeroy")
Annotations marking the peak week
# HINT — Plotly filled area:
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["week_start"],
    y=df["rental_count"],
    fill="tozeroy",
    mode="lines+markers",
    hovertemplate="Week: %{x}<br>Rentals: %{y}<br>Customers: %{customdata}<extra></extra>",
    customdata=df["unique_customers"],
    name="Weekly Rentals",
))

# Find peak week and annotate:
peak_idx = df["rental_count"].idxmax()
fig.add_annotation(
    x=df.loc[peak_idx, "week_start"],
    y=df.loc[peak_idx, "rental_count"],
    text=f"Peak: {df.loc[peak_idx, 'rental_count']:,}",
    showarrow=True, arrowhead=2,
)
"""
from pathlib import Path
import sys
import pandas as pd
import streamlit as st
import plotly.express as px 
import plotly.graph_objects as go
_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_root / "sprint-01" / "day-02"))
sys.path.insert(1, str(_root / "sprint-05" / "day-31"))

from db import load_rental_stats

PLOTLY_THEME = "plotly_white"

def render():
  st.title("📅 Rental Analytics")
  st.markdown("---")
  df = load_rental_stats()

  # KPIs
  col1, col2, col3 = st.columns(3)
  with col1:
      st.metric("Total Rentals", f"{df['rental_count'].sum():,}")
  with col2:
      st.metric("Open Rentals", f"{df['still_open'].sum():,}")
  with col3:
      st.metric("Unique Customers", df["unique_customers"].iloc[0])

  # Line chart
  fig = px.line(df, x="week_start", y="rental_count",
                title="Weekly Rental Count", template=PLOTLY_THEME)
  st.plotly_chart(fig, use_container_width=True)

  #-- HINT — Plotly filled area:
  st.markdown("---")
  st.subheader("Weekly Rental Trend")
  df = load_rental_stats()
  fig = go.Figure()
  fig.add_trace(go.Scatter(
      x=df["week_start"],
      y=df["rental_count"],
        fill="tozeroy",
      mode="lines+markers",
        hovertemplate="Week: %{x}<br>Rentals: %{y}<br>Customers: %{customdata}<extra></extra>",
      customdata=df["unique_customers"],
      name="Weekly Rentals",
  ))
    # Find peak week and annotate:
  peak_idx = df["rental_count"].idxmax()
  fig.add_annotation(
            x=df.loc[peak_idx, "week_start"],
            y=df.loc[peak_idx, "rental_count"],
            text=f"Peak: {df.loc[peak_idx, 'rental_count']:,}",
            showarrow=True, arrowhead=2,
        )
  st.plotly_chart(fig, use_container_width=True)  
  
  st.markdown("*(Hover over points for details)*")

    