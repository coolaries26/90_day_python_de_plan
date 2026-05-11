"""
components.py — Reusable Streamlit Components
==============================================
YOUR TASK: Implement 3 reusable UI helper functions.

HINTS:

def kpi_row(metrics: list[dict]) -> None:
    '''
    Display a row of metric cards.
    metrics = [{"label": "Customers", "value": 599, "delta": None}, ...]
    '''
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.metric(
            label=m["label"],
            value=m["value"],
            delta=m.get("delta"),
            delta_color=m.get("delta_color", "normal"),
        )

def status_table(df: pd.DataFrame, status_col: str = "status") -> None:
    '''
    Display a DataFrame with coloured status column.
    '''
    COLOR_MAP = {"success": "green", "failed": "red",
                 "sla_miss": "orange", "pending": "grey"}
    def colour_status(val):
        c = COLOR_MAP.get(str(val).lower(), "grey")
        return f"color: {c}; font-weight: bold"

    st.dataframe(
        df.style.applymap(colour_status, subset=[status_col]),
        use_container_width=True,
    )

def section_header(title: str, subtitle: str = "") -> None:
    '''Display a consistent section header with optional subtitle.'''
    st.subheader(title)
    if subtitle:
        st.caption(subtitle)
    st.markdown("---")
"""
# YOUR CODE HERE
import streamlit as st
import pandas as pd

def kpi_row(metrics: list[dict]) -> None:
    '''
    Display a row of metric cards.
    metrics = [{"label": "Customers", "value": 599, "delta": None}, ...]
    '''
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.metric(
            label=m["label"],
            value=m["value"],
            delta=m.get("delta"),
            delta_color=m.get("delta_color", "normal"),
        )

def status_table(df: pd.DataFrame, status_col: str = "status") -> None:
    '''
    Display a DataFrame with coloured status column.
    '''
    COLOR_MAP = {"success": "green", "failed": "red",
                 "sla_miss": "orange", "pending": "grey"}
    def colour_status(val):
        c = COLOR_MAP.get(str(val).lower(), "grey")
        return f"color: {c}; font-weight: bold"

    st.dataframe(
        df.style.applymap(colour_status, subset=[status_col]),
        use_container_width=True,
    )

def section_header(title: str, subtitle: str = "") -> None:
    '''Display a consistent section header with optional subtitle.'''
    st.subheader(title)
    if subtitle:
        st.caption(subtitle)
    st.markdown("---")
