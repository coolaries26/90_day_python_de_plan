"""ml_insights.py — ML Predictions + Model Insights."""
import sys
import json
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from an_db import load_churn_predictions, load_delay_predictions


MODEL_DIR = Path(__file__).resolve().parent.parent.parent / "ml" / "models"


def render():
    st.title("🤖 ML Model Insights")

    tab1, tab2, tab3 = st.tabs(["Churn Risk", "Delivery Delay", "Model Info"])

    # ── Tab 1: Churn Risk ─────────────────────────────────────────────────
    with tab1:
        st.subheader("Customer Churn Predictions")
        churn_df = load_churn_predictions()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Customers",    f"{len(churn_df):,}")
        col2.metric("Predicted Churned",  f"{churn_df['predicted_churn'].sum():,}")
        col3.metric("Churn Rate",
                    f"{churn_df['predicted_churn'].mean():.1%}")

        # Churn probability distribution
        fig = px.histogram(
            churn_df, x="churn_probability", nbins=30,
            title="Churn Probability Distribution",
            color_discrete_sequence=["tomato"],
            template="plotly_dark",
        )
        fig.add_vline(x=0.5, line_dash="dash", line_color="white",
                      annotation_text="Decision threshold (0.5)")
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Delivery Delay ─────────────────────────────────────────────
    with tab2:
        st.subheader("Delivery Delay Predictions")
        delay_df = load_delay_predictions()

        col1, col2, col3 = st.columns(3)
        late_col = [c for c in delay_df.columns if "late" in c.lower() and "predict" in c.lower()]
        if late_col:
            col1.metric("Total Orders",        f"{len(delay_df):,}")
            col2.metric("Predicted Late",      f"{delay_df[late_col[0]].sum():,}")
            col3.metric("Predicted Late Rate", f"{delay_df[late_col[0]].mean():.1%}")

        # Late probability distribution
        prob_col = [c for c in delay_df.columns if "prob" in c.lower()]
        if prob_col:
            fig2 = px.histogram(
                delay_df, x=prob_col[0], nbins=30,
                title="Late Delivery Probability Distribution",
                color_discrete_sequence=["orange"],
                template="plotly_dark",
            )
            st.plotly_chart(fig2, use_container_width=True)

    # ── Tab 3: Model Info ─────────────────────────────────────────────────
    with tab3:
        st.subheader("Model Metadata")

        for model_name in ["churn_pipeline", "delay_pipeline"]:
            meta_path = MODEL_DIR / f"{model_name}_metadata.json"
            if meta_path.exists():
                with open(meta_path) as f:
                    meta = json.load(f)
                with st.expander(f"📋 {model_name}", expanded=True):
                    st.json(meta)

        st.markdown("---")
        st.info(
            "**Model Limitations:**\n\n"
            "- Churn model CV F1 ≈ 0.22 — limited by feature availability "
            "(purchase history excluded to prevent leakage)\n"
            "- Delay model optimised for recall over precision — "
            "better to flag potential delays early\n"
            "- Both models trained on 2016-2018 data — "
            "seasonal patterns may have shifted"
        )
        st.markdown("---")