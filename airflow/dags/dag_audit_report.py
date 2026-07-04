"""
dag_audit_report.py — Day 23 | Pipeline Audit Report DAG
=========================================================
Runs after customer and film ETL DAGs complete.
Reads pipeline config from Variables.
Queries etl_audit_log table.
Writes summary report to CSV.
"""
from __future__ import annotations
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

# Dynamic Windows IP
import subprocess
_ip = subprocess.run(
    ["bash", "-c",
     "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path(os.environ.get(
    "project_root", "/mnt/d/alsgit/90_day_python_de_plan"
))
for p in [
    PROJECT_ROOT / "sprint-01" / "day-02", # db_utils
    PROJECT_ROOT / "sprint-01" / "day-04",  # etl_protocols
]:
    sys.path.insert(0, str(p))

from airflow import DAG
from airflow.models import Variable #type: ignore
from airflow.operators.python import PythonOperator #type: ignore

default_args = {
    "owner": "python-de-journey",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 1,            # ← lowest priority
    "weight_rule": "absolute",
}


# ── Q1: Read audit log — fully provided ───────────────────────────────────
def read_audit_log(**context) -> None:
    """
    Read recent pipeline runs from etl_audit_log.
    Push summary dict to XCom for downstream tasks.
    """
    import pandas as pd
    from db_utils import get_engine, dispose_engine #type: ignore

    engine = get_engine()
    sql = """
        SELECT
            pipeline_name,
            source_table,
            target_table,
            rows_loaded,
            status,
            elapsed_s,
            run_at
        FROM etl_audit_log
        ORDER BY run_at DESC
        LIMIT 20
    """
    df = pd.read_sql(sql, engine)
    dispose_engine()

    # Build summary — primitives only for XCom
    summary = {
        "total_runs":    int(len(df)),
        "successful":    int((df["status"] == "success").sum()),
        "failed":        int((df["status"] == "failed").sum()),
        "total_rows":    int(df["rows_loaded"].sum()),
        "pipelines":     df["pipeline_name"].unique().tolist(),
        "latest_run":    str(df["run_at"].max()) if len(df) > 0 else None,
    }

    context["ti"].xcom_push(key="audit_summary", value=summary)
    print(f"Audit log read | {summary}")


# ── Q1: Check pipeline row counts — fully provided ────────────────────────
def check_pipeline_counts(**context) -> None:
    """
    Read pipeline config from Variable (JSON).
    Verify each target table has expected row count.
    Push pass/fail per pipeline to XCom.
    """
    from db_utils import execute_scalar, close_pool #type: ignore

    # Read structured config from Variable
    config_json = Variable.get("etl_pipeline_config", default_var="{}")
    config = json.loads(config_json)

    results = {}
    for name, cfg in config.items():
        try:
            count = execute_scalar(
                f"SELECT COUNT(*) FROM {cfg['target']}"
            )
            passed = count >= cfg["min_rows"]
            results[name] = {
                "table":    cfg["target"],
                "count":    int(count),
                "min_rows": cfg["min_rows"],
                "passed":   passed,
            }
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{name}: {count} rows {status}")
        except Exception as exc:
            results[name] = {"error": str(exc), "passed": False}
            print(f"{name}: ERROR — {exc}")

    close_pool()
    context["ti"].xcom_push(key="count_checks", value=results)


# ── Q2: Write report — WRITE THIS YOURSELF ───────────────────────────────
def write_audit_report(**context) -> None:
    """
    Q2 — YOUR TASK:
    Pull XCom from upstream tasks and write a CSV + Markdown report.

    HINTS:
    Step 1: Pull audit_summary from read_audit_log task
        summary = context["ti"].xcom_pull(
            task_ids="read_audit_log", key="audit_summary"
        )

    Step 2: Pull count_checks from check_pipeline_counts task
        counts = context["ti"].xcom_pull(
            task_ids="check_pipeline_counts", key="count_checks"
        )

    Step 3: Get output dir from Variable
        output_dir = Path(Variable.get(
            "project_root",
            default_var="/mnt/d/alsgit/90_day_python_de_plan"
        )) / "airflow" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

    Step 4: Write Markdown report to output_dir / "audit_report.md"
        Content should include:
          - Report timestamp
          - Summary: total_runs, successful, failed, total_rows
          - Per-pipeline count check (pass/fail)

    Step 5: Write CSV of count_checks to output_dir / "count_checks.csv"
        import pandas as pd
        df = pd.DataFrame(counts).T.reset_index()
        df.columns = ["pipeline"] + list(df.columns[1:])
        df.to_csv(output_dir / "count_checks.csv", index=False)

    Step 6: Print confirmation with file paths

    Self-check:
      - audit_report.md exists in airflow/output/
      - count_checks.csv exists in airflow/output/
      - Both pipelines show ✅ PASS in the report
    """
    # YOUR CODE HERE
    import pandas as pd
    from pathlib import Path
    from airflow.models import Variable #type: ignore

    # Pull XCom from upstream tasks
    summary = context["ti"].xcom_pull(
        task_ids="read_audit_log", key="audit_summary"
    )
    counts = context["ti"].xcom_pull(
        task_ids="check_pipeline_counts", key="count_checks"
    )
    # Get output dir from Variable
    output_dir = Path(Variable.get(
        "PROJECT_ROOT",
        default_var="/mnt/d/alsgit/90_day_python_de_plan"
    )) / "airflow" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write Markdown report
    report_path = output_dir / "audit_report.md"
    with open(report_path, "w") as f:
        f.write("# Audit Report\n\n")
        f.write(f"**Report generated at:** {datetime.now()}\n\n")
        f.write("## Summary\n")
        f.write(f"- Total Runs: {summary['total_runs']}\n")
        f.write(f"- Successful: {summary['successful']}\n")
        f.write(f"- Failed: {summary['failed']}\n")
        f.write(f"- Total Rows Loaded: {summary['total_rows']}\n")
        f.write(f"- Pipelines: {', '.join(summary['pipelines'])}\n")
        f.write(f"- Latest Run: {summary['latest_run']}\n\n")

        f.write("## Pipeline Count Checks\n")
        for name, result in counts.items():
            if "error" in result:
                f.write(f"- **{name}**: ERROR — {result['error']}\n")
            else:
                status = "✅ PASS" if result["passed"] else "❌ FAIL"
                f.write(
                    f"- **{name}**: {result['count']} rows "
                    f"(min {result['min_rows']}) {status}\n"
                )
    # Write CSV of count_checks
    csv_path = output_dir / "count_checks.csv"
    df = pd.DataFrame(counts).T.reset_index()
    df.columns = ["pipeline"] + list(df.columns[1:])
    df.to_csv(csv_path, index=False)

    # Print confirmation    
    print(f"Audit report written to {report_path}")
    print(f"Count checks written to {csv_path}")

with DAG(
    dag_id="pipeline_audit_report",
    description="Daily audit report across all ETL pipelines",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["audit", "report", "sprint-04"],
) as dag:

    task_read_log = PythonOperator(
        task_id="read_audit_log",
        python_callable=read_audit_log,
    )

    task_check_counts = PythonOperator(
        task_id="check_pipeline_counts",
        python_callable=check_pipeline_counts,
    )

    task_write_report = PythonOperator(
        task_id="write_audit_report",
        python_callable=write_audit_report,
    )

    # read_log and check_counts run in parallel, then write_report
    [task_read_log, task_check_counts] >> task_write_report