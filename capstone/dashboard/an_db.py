"""capstone/dashboard/db.py — Cached queries for e-commerce dashboard."""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import streamlit as st
PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PATH))
print(f"DEBUG: Added {PATH} to sys.path")
from db import get_ecommerce_engine


def _engine():
    return get_ecommerce_engine()


@st.cache_data(ttl=300)
def load_customer_ltv() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM analytics.customer_ltv", _engine())


@st.cache_data(ttl=300)
def load_order_metrics() -> pd.DataFrame:
    return pd.read_sql("""
        SELECT * FROM analytics.order_metrics
        WHERE delivery_days_actual IS NOT NULL
    """, _engine())


@st.cache_data(ttl=300)
def load_seller_performance() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM analytics.seller_performance ORDER BY total_revenue DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_product_analytics() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM analytics.product_analytics ORDER BY total_revenue DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_monthly_revenue() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM analytics.monthly_revenue ORDER BY order_month",
        _engine()
    )


@st.cache_data(ttl=300)
def load_churn_predictions() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM ml.churn_predictions", _engine())


@st.cache_data(ttl=300)
def load_delay_predictions() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM ml.delay_predictions", _engine())


@st.cache_data(ttl=60)
def get_kpis() -> dict:
    customers = load_customer_ltv()
    orders    = load_order_metrics()
    monthly   = load_monthly_revenue()
    churn     = load_churn_predictions()

    return {
        "total_customers":   len(customers),
        "total_orders":      len(orders),
        "orders_count":      float(monthly["total_orders"].sum()),
        "total_revenue":     float(monthly["total_revenue"].sum()),
        "avg_review_score":  float(orders["review_score"].mean()),
        "late_delivery_rate":float(orders["is_late"].mean()),
        "churn_rate":        float(churn["predicted_churn"].mean())
            if "predicted_churn" in churn.columns else 0.0,
        "avg_ltv":           float(customers["total_spent"].mean()),
        "repeat_customers":  int((customers["is_churned"] == 0).sum()),
    }