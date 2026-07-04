"""
dag_chart_generator.py — Day 34 | Chart Generation DAG
=======================================================
Triggered automatically when customer + film ETL datasets update.
Regenerates all static (PNG) and interactive (HTML) charts.
Writes a manifest of generated files.

Trigger: Dataset events from customer_etl_daily + film_etl_daily
"""
from __future__ import annotations
import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure

# Dynamic Windows IP
_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/d/alsgit/90_day_python_de_plan")

# Add all required paths
for p in [
    PROJECT_ROOT / "sprint-01" / "day-02", # for db_utils and get_engine
    PROJECT_ROOT / "sprint-01" / "day-04", # for models_compat and AuditLog
    PROJECT_ROOT / "sprint-05" / "day-29", # for static charts
    PROJECT_ROOT / "sprint-05" / "day-30", # for interactive charts
    PROJECT_ROOT / "sprint-03" / "day-16", # for Dataset and airflow callbacks
    PROJECT_ROOT / "sprint-05" / "day-35", # for test_charts and rental timeline chart function
]:
    sys.path.insert(0, str(p))


# ── Datasets ──────────────────────────────────────────────────────────────
CUSTOMER_DATASET = Dataset("postgresql://dvdrental/analytics_customer_airflow")
FILM_DATASET     = Dataset("postgresql://dvdrental/analytics_film_airflow")
CHART_DATASET    = Dataset("file:///mnt/d/alsgit/90_day_python_de_plan/airflow/output/charts")

# Output directory for Airflow-managed charts
CHART_OUTPUT = PROJECT_ROOT / "airflow" / "output" / "charts"
CHART_OUTPUT.mkdir(parents=True, exist_ok=True)

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure,
}


# ── Q1: Generate static charts — fully provided ───────────────────────────
def generate_static_charts(**context) -> dict:
    """
    Run all 5 matplotlib/seaborn charts from charts.py.
    Saves PNGs to airflow/output/charts/static/
    """
    import matplotlib
    matplotlib.use("Agg")

    # Import chart functions
    sys.path.insert(0, str(PROJECT_ROOT / "sprint-05" / "day-29"))

    # Override OUTPUT_DIR in charts module
    import charts as charts_module
    import test_charts as test_module
    static_dir = CHART_OUTPUT / "static"
    static_dir.mkdir(exist_ok=True)
    charts_module.OUTPUT_DIR = static_dir
    test_module.OUTPUT_DIR = static_dir


    generated = []
    chart_functions = [
        charts_module.chart_customer_segments,
        charts_module.chart_monthly_revenue,
        charts_module.chart_film_value_tiers,
        charts_module.chart_pipeline_history,
        charts_module.chart_customer_spend_distribution,
        test_module.chart_rental_timeline,   # from day 35 test_charts.py — bonus!
    ]

    for fn in chart_functions:
        try:
            path = fn()
            generated.append(str(path))
            print(f"✅ {fn.__name__} → {path.name}")
        except Exception as exc:
            print(f"❌ {fn.__name__} failed: {exc}")

    print(f"\nStatic charts: {len(generated)}/5 generated")
    context["ti"].xcom_push(key="static_charts", value=generated)
    return {"count": len(generated), "paths": generated}


# ── Q2: Generate interactive charts — WRITE THIS YOURSELF ─────────────────
def generate_interactive_charts(**context) -> dict:
    """
    Q2 — YOUR TASK:
    Run all 4 Plotly charts from plotly_charts.py.
    Saves HTML + PNG to airflow/output/charts/interactive/

    HINTS:
    Step 1: Import plotly_charts module
        sys.path.insert(0, str(PROJECT_ROOT / "sprint-05" / "day-30"))
        import plotly_charts as plotly_module

    Step 2: Override OUTPUT_DIR in the module
        interactive_dir = CHART_OUTPUT / "interactive"
        interactive_dir.mkdir(exist_ok=True)
        plotly_module.OUTPUT_DIR = interactive_dir

    Step 3: Run each chart function
        chart_functions = [
            plotly_module.p1_customer_segments,
            plotly_module.p2_monthly_revenue,
            plotly_module.p3_spend_vs_rentals,
            plotly_module.p4_category_treemap,
        ]
        generated = []
        for fn in chart_functions:
            try:
                fig = fn()
                generated.append(fn.__name__)
                print(f"✅ {fn.__name__}")
            except Exception as exc:
                print(f"❌ {fn.__name__}: {exc}")

    Step 4: Push to XCom
        context["ti"].xcom_push(key="interactive_charts", value=generated)

    Step 5: Return dict {"count": len(generated), "charts": generated}

    Self-check: 4 HTML files appear in airflow/output/charts/interactive/
    """
    # YOUR CODE HERE
#step 1: import module
    sys.path.insert(0, str(PROJECT_ROOT / "sprint-05" / "day-30"))
    import plotly_charts as plotly_module
#step 2: override output dir
    interactive_dir = CHART_OUTPUT / "interactive"
    interactive_dir.mkdir(exist_ok=True)
    plotly_module.OUTPUT_DIR = interactive_dir
#step 3: run each chart function
    chart_functions = [
        plotly_module.p1_customer_segments,
        plotly_module.p2_monthly_revenue,
        plotly_module.p3_spend_vs_rentals,
        plotly_module.p4_category_treemap,
    ]
    generated = []
    for fn in chart_functions:
        try:
            fig = fn()
            generated.append(fn.__name__)
            print(f"✅ {fn.__name__}")
        except Exception as exc:
            print(f"❌ {fn.__name__}: {exc}")

# step 4: Push to XCom
    context["ti"].xcom_push(key="interactive_charts", value=generated)

# step 5: Return dict
    return {"count": len(generated), "charts": generated}


# ── Write manifest ────────────────────────────────────────────────────────
def write_chart_manifest(**context) -> None:
    """
    Write a JSON manifest of all generated charts.
    Used by downstream DAGs and the Streamlit app to know which charts exist.
    """
    static    = context["ti"].xcom_pull(
        task_ids="generate_static_charts",    key="static_charts"
    ) or []
    interactive = context["ti"].xcom_pull(
        task_ids="generate_interactive_charts", key="interactive_charts"
    ) or []

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "static_charts":      static,
        "interactive_charts": interactive,
        "total_charts":       len(static) + len(interactive),
        "chart_output_dir":   str(CHART_OUTPUT),
    }

    manifest_path = CHART_OUTPUT / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Manifest written → {manifest_path}")
    print(f"Total charts: {manifest['total_charts']}")

    # Write audit log entry
    try:
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine
        from models_compat import AuditLog

        engine = get_engine()
        audit = AuditLog(
            pipeline_name="dag_chart_generator",
            source_table="analytics_customer_airflow,analytics_film_airflow",
            target_table="charts_output",
            rows_loaded=manifest["total_charts"],
            status="success",
        )
        with Session(engine) as session:
            session.add(audit)
            session.commit()
        dispose_engine()
        print("Audit log entry written")
    except Exception as exc:
        print(f"Audit log failed (non-fatal): {exc}")


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_chart_generator",
    description="Auto-generate charts when ETL datasets update",
    default_args=default_args,
    schedule=[CUSTOMER_DATASET, FILM_DATASET],   # dataset trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["charts", "visualization", "sprint-05"],
) as dag:

    task_static = PythonOperator(
        task_id="generate_static_charts",
        python_callable=generate_static_charts,
        pool="db_connection_pool",
    )

    task_interactive = PythonOperator(
        task_id="generate_interactive_charts",
        python_callable=generate_interactive_charts,
        pool="db_connection_pool",
    )

    task_manifest = PythonOperator(
        task_id="write_chart_manifest",
        python_callable=write_chart_manifest,
        outlets=[CHART_DATASET],         # signals charts are fresh
        trigger_rule="all_done",
    )

    # Static and interactive charts run in parallel → manifest after both
    [task_static, task_interactive] >> task_manifest