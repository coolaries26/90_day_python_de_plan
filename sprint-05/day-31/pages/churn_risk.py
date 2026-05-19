"""
churn_risk.py — Churn Risk Streamlit Page
==========================================
YOUR TASK: Build churn risk analytics page.

Requirements:
1. Page title: "⚠️ Churn Risk"

2. Load predictions from Day 36 predictions.csv:
   predictions_path = Path(__file__).resolve().parent.parent.parent
       / "sprint-06" / "day-36" / "output" / "predictions.csv"
   pred_df = pd.read_csv(predictions_path)

3. Load customer data from db.py:
   customers = load_customers()
   # Merge predictions with customer names if possible
   # customers has customer_id, pred_df doesn't — use index alignment
   # or just show predictions as-is

4. 3 KPI metrics:
   - Predicted Active:  pred_df["predicted_active"].sum()
   - Predicted Churned: (pred_df["predicted_active"] == 0).sum()
   - Model Accuracy:    f"{pred_df['correct'].mean():.1%}"

5. Churn risk histogram:
   px.histogram(pred_df, x="churn_probability", nbins=20,
                title="Churn Probability Distribution",
                color_discrete_sequence=["tomato"])

6. High risk customers table (churn_probability > 0.5):
   high_risk = pred_df[pred_df["churn_probability"] > 0.5]
   st.warning(f"⚠️ {len(high_risk)} customers at high churn risk")
   st.dataframe(high_risk, use_container_width=True)

7. Model comparison table (load from model_comparison.csv if exists):
   comp_path = Path(...) / "sprint-06" / "day-37" / "output" / "model_comparison.csv"
   if comp_path.exists():
       comp_df = pd.read_csv(comp_path)
       st.subheader("Model Comparison")
       st.dataframe(comp_df, use_container_width=True)

8. Add to app.py sidebar:
   elif page == "⚠️ Churn Risk":
       import pages.churn_risk as churn
       churn.render()
"""
# YOUR CODE HERE
import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
import sys
from sklearn.metrics import (accuracy_score, f1_score, precision_score, recall_score)
from sklearn.model_selection import train_test_split
from pathlib import Path
_root=Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_root.parent / "sprint-01" / "day-02"))
sys.path.insert(1, str(_root.parent / "sprint-06" / "day-36"))
sys.path.insert(0, str(_root.parent / "sprint-05" / "day-31"))
sys.path.insert(0, str(_root.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(_root.parent / "sprint-06" / "day-37"))

from db import load_customers, _engine
from random_forest import train_random_forest
from logger import get_pipeline_logger
logger = get_pipeline_logger(__name__)

# HINTS:

def render():
    st.title("⚠️ Churn Risk")
    st.markdown("---")
# Load drift log from DB:
    try:
        drift_df = pd.read_sql(
            "SELECT * FROM ml_drift_log ORDER BY checked_at DESC LIMIT 5",
            _engine()
        )
    except Exception:
        drift_df = pd.DataFrame()
# Show alert banner if drift detected:
    if not drift_df.empty:
        latest = drift_df.iloc[0]
        if latest["status"] == "drift":
            st.error(f"🚨 MODEL DRIFT DETECTED: {latest['message']}")
        elif latest["status"] == "warning":
            st.warning(f"⚠️ Accuracy declining: {latest['message']}")
        else:
            st.success("✅ Model performing normally")
    else:
        st.info("No drift checks recorded yet")
# Step 1: Load predictions and customer data
    predictions_path = _root.parent / "sprint-06" / "day-36" / "output" / "predictions.csv"
    pred_df = pd.read_csv(predictions_path) 
    customers = load_customers()
# Step 2: Show KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Predicted Active", pred_df["predicted_active"].sum())
    col2.metric("Predicted Churned", (pred_df["predicted_active"] == 0).sum())
    col3.metric("Model Accuracy", f"{pred_df['correct'].mean():.1%}")
# Step 3: Churn risk histogram
    fig = px.histogram(pred_df, x="churn_probability", nbins=20,
                        title="Churn Probability Distribution",
                        color_discrete_sequence=["tomato"])
    st.plotly_chart(fig)
# Step 4: High risk customers table
    high_risk = pred_df[pred_df["churn_probability"] > 0.5]
    st.warning(f"⚠️ {len(high_risk)} customers at high churn risk")
    st.dataframe(high_risk, use_container_width=True)   
# Step 5: Model comparison table
    comp_path = _root.parent / "sprint-06" / "day-37" / "output" / "model_comparison.csv"
    if comp_path.exists():
        comp_df = pd.read_csv(comp_path)
        st.subheader("Model Comparison")
        st.dataframe(comp_df, use_container_width=True)
#step 6: If model_comparison.csv doesn't exist, train RandomForest and create it
    else:
        st.info("Model comparison not found. Training RandomForest and creating model_comparison.csv...")
        # Train/test split
        X = pred_df.drop(columns=["customer_id", "predicted_active", "correct"])
        y = pred_df["predicted_active"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        # Train RandomForest
        rf_model = train_random_forest(X_train, y_train)
        rf_pred = rf_model.predict(X_test)
        # Build comparison DataFrame
        results = []
        for name, pred in [("RandomForest", rf_pred)]:
            results.append({
                "Model":     name,
                "Accuracy":  round(accuracy_score(y_test, pred), 4),
                "F1":        round(f1_score(y_test, pred, average="weighted"), 4),
                "Precision": round(precision_score(y_test, pred, average="weighted", zero_division=0), 4),
                "Recall":    round(recall_score(y_test, pred, average="weighted"), 4),
            })
        comp_df = pd.DataFrame(results)
        # Save and print
        OUTPUT_DIR = Path(__file__).parent / "sprint-05" / "day-31" / "output"
        OUTPUT_DIR.mkdir(exist_ok=True)

        comp_df.to_csv(OUTPUT_DIR / "model_comparison.csv", index=False)
        logger.info(f"\n── Model Comparison ──────────────────────")
        logger.info(f"\n{comp_df.to_string(index=False)}")
        st.subheader("Model Comparison")
        st.dataframe(comp_df, use_container_width=True)