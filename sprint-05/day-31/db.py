"""
db.py — Streamlit Database Layer
==================================
Cached query functions for the DVD Rental dashboard.
@st.cache_data(ttl=300) prevents re-querying on every widget interaction.

All functions return DataFrames or dicts — no DB objects leak out.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
import pandas as pd
import streamlit as st

# Add project paths
_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_root / "sprint-01" / "day-02"))

from db_utils import get_engine


def _engine():
    """Get or create SQLAlchemy engine. Reuses singleton from db_utils."""
    return get_engine()


@st.cache_data(ttl=300)   # cache 5 minutes — refresh every 5 min
def load_customers() -> pd.DataFrame:
    """Load full customer analytics table."""
    return pd.read_sql(
        "SELECT * FROM analytics_customer_airflow ORDER BY total_spend DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_films() -> pd.DataFrame:
    """Load film analytics table."""
    return pd.read_sql(
        " select a.rental_count,a.value_score,b.* from analytics_film_value_score a join analytics_film_airflow b on a.film_id=b.film_id ORDER BY value_tier DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_monthly_revenue() -> pd.DataFrame:
    """Load monthly enriched revenue table."""
    return pd.read_sql(
        """SELECT payment_date, total_revenue, payment_count,
                  avg_payment, mom_growth_pct
           FROM analytics_monthly_enriched
           ORDER BY payment_date""",
        _engine()
    )


@st.cache_data(ttl=60)    # pipeline status refreshes every minute
def load_pipeline_status() -> pd.DataFrame:
    """Load recent pipeline run history from audit log."""
    return pd.read_sql(
        """SELECT pipeline_name, status, rows_loaded,
                  elapsed_s, run_at
           FROM etl_audit_log
           ORDER BY run_at DESC
           LIMIT 20""",
        _engine()
    )


@st.cache_data(ttl=300)
def get_summary_kpis() -> dict[str, Any]:
    """Return top-level KPI values for overview page."""
    customers = load_customers()
    films     = load_films()
    monthly   = load_monthly_revenue()
    pipeline  = load_pipeline_status()

    return {
        "total_customers":   len(customers),
        "active_customers":  int(customers["is_active"].sum())
            if "is_active" in customers.columns else 0,
        "total_films":       len(films),
        "total_revenue":     float(monthly["total_revenue"].sum()),
        "pipeline_runs":     len(pipeline),
        "successful_runs":   int((pipeline["status"] == "success").sum()),
        "failed_runs":       int((pipeline["status"] == "failed").sum()),
        "platinum_customers":int((customers["value_segment"] == "Platinum").sum())
            if "value_segment" in customers.columns else 0,
    }