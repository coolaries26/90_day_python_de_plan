"""Overview page — KPI cards + pipeline status."""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_summary_kpis, load_pipeline_status


def render():
    st.title("📊 Overview")
    st.markdown("Real-time summary from the dvdrental analytics pipeline.")

    # ── KPI Cards ────────────────────────────────────────────────────────
    kpis = get_summary_kpis()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers",   kpis["total_customers"])
    col2.metric("Total Films",       kpis["total_films"])
    col3.metric("Total Revenue",     f"${kpis['total_revenue']:,.2f}")
    col4.metric("Platinum Customers",kpis["platinum_customers"])

    st.markdown("---")
    col5, col6, col7 = st.columns(3)
    col5.metric("Pipeline Runs",    kpis["pipeline_runs"])
    col6.metric("Successful Runs",  kpis["successful_runs"],
                delta=f"+{kpis['successful_runs']}", delta_color="normal")
    col7.metric("Failed Runs",      kpis["failed_runs"],
                delta=f"-{kpis['failed_runs']}" if kpis["failed_runs"] else "0",
                delta_color="inverse")

    # ── Pipeline Status Table ──────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Recent Pipeline Runs")

    pipeline_df = load_pipeline_status()

    # Colour status column
    def colour_status(val):
        colour = {"success": "green", "failed": "red", "sla_miss": "orange"}
        c = colour.get(val, "grey")
        return f"color: {c}; font-weight: bold"

    st.dataframe(
        pipeline_df.style.applymap(colour_status, subset=["status"]),
        use_container_width=True,
        height=350,
    )

    # ── Pipeline Success Rate Gauge ────────────────────────────────────────
    st.markdown("---")
    st.subheader("Pipeline Success Rate")

    total = kpis["pipeline_runs"]
    success_pct = (kpis["successful_runs"] / total * 100) if total > 0 else 0

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=success_pct,
        number={"suffix": "%"},
        delta={"reference": 80, "valueformat": ".1f"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar":  {"color": "green" if success_pct >= 80 else "red"},
            "steps": [
                {"range": [0, 50],   "color": "lightcoral"},
                {"range": [50, 80],  "color": "lightyellow"},
                {"range": [80, 100], "color": "lightgreen"},
            ],
            "threshold": {"line": {"color": "black", "width": 2},
                         "thickness": 0.75, "value": 80},
        },
        title={"text": "Success Rate vs 80% Target"},
    ))
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)