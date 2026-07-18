# 📅 DAY 60 — Sprint 09 | Airflow + GX Integration
## Add GX Checkpoint Task to ETL Pipeline, Fail Fast on Bad Data

---

## 🔁 RETROSPECTIVE — Day 59

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Checkpoint created | ✅ Pass | |
| run_checkpoint.py | ✅ Pass | Per-suite results + exit code |
| 29/29 passing | ✅ Pass | Single command |
| Data Docs updated | ✅ Pass | |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-09/day-60-airflow-gx
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-12: Data Quality & Great Expectations |
| Story           | ST-60: Airflow + GX Integration |
| Task ID         | TASK-060 |
| Sprint          | Sprint 09 (Days 57–64) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | airflow, great-expectations, checkpoint, data-quality, day-60 |
| Acceptance Criteria | GX checkpoint runs as Airflow task after dbt; failure triggers audit log entry; pipeline stops if data quality fails |

---

## 📚 BACKGROUND

### Where GX Fits in the Pipeline

```
Current pipeline (dag_ecommerce_etl + dag_dbt_pipeline):
  raw load → analytics ETL → churn model → delay model → dbt run → dbt test

After today:
  raw load → analytics ETL → churn model → delay model
                                              ↓
                                          dbt run → dbt test
                                              ↓
                                     GX checkpoint ← NEW
                                              ↓
                                     ✅ All good → done
                                     ❌ Failure  → alert + stop

Why AFTER dbt test, not before?
  dbt test: schema-level (unique, not_null, FK constraints)
  GX:       business-level (mean within range, proportion checks)
  Both layers catch different problems — run both.
```

### Two Integration Patterns

```
Pattern A — BashOperator (simple, runs run_checkpoint.py):
  task_gx = BashOperator(
      task_id="run_gx_checkpoint",
      bash_command="python /mnt/c/.../run_checkpoint.py",
  )
  # Pros: easy, reuses your existing script
  # Cons: GX runs in Windows Python, not WSL2

Pattern B — PythonOperator (cleaner, runs in Airflow's Python):
  def run_gx(**context):
      import great_expectations as gx
      ctx = gx.data_context.DataContext(...)
      result = ctx.run_checkpoint("ecommerce_data_quality")
      if not result.success:
          raise ValueError("GX checkpoint failed — data quality issues detected")
  
  task_gx = PythonOperator(
      task_id="run_gx_checkpoint",
      python_callable=run_gx,
  )
  # Pros: proper Python, failure raises exception → Airflow catches it
  # Cons: GX must be installed in airflow-venv
```

**Use Pattern B** — PythonOperator integrates properly with Airflow's retry and callback system.

---

## 🎯 OBJECTIVES

1. Install GX in WSL2 `airflow-venv`
2. Add `run_gx_checkpoint` task to `dag_dbt_pipeline.py`
3. Task fails (raises exception) if any expectation fails
4. `on_failure_callback` logs GX failure to `etl_audit_log`
5. Trigger full pipeline and verify GX task runs
6. Push clean `[DAY-060][S09]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 20 min | Install GX in WSL2 + test |
| B | 50 min | Add GX task to dag_dbt_pipeline.py |
| C | 20 min | Trigger + verify |
| D | 10 min | Simulate failure + verify alert |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install GX in WSL2 (Block A)

```bash
# WSL2
source ~/airflow-venv/bin/activate
pip install great-expectations==0.18.19

# Verify
python -c "import great_expectations as gx; print(gx.__version__)"
# Expected: 0.18.x

# Test checkpoint runs from WSL2
cd /mnt/c/90_day_python_de_plan
python sprint-09/day-59/run_checkpoint.py
# Should show 29/29 passing
```

If connection fails from WSL2 (127.0.0.1 → Windows IP issue), update the datasource connection string in GX:

```python
# The GX datasource was configured with host=127.0.0.1
# From WSL2, this won't reach Windows PostgreSQL
# Check the datasource config in:
# sprint-09/day-57/gx_project/great_expectations/great_expectations.yml

# If needed, add WSL2-compatible datasource:
# Update connection_string to use Windows IP
WINDOWS_IP=$(ip route | grep default | awk '{print $3}')
# Change: postgresql+psycopg2://appuser:AppUser%402024%21@127.0.0.1:5432/ecommerce_db
# To:     postgresql+psycopg2://appuser:AppUser%402024%21@${WINDOWS_IP}:5432/ecommerce_db
```

---

### EXERCISE 2 — Add GX Task to dag_dbt_pipeline.py (Block B)
**[Function provided. Task wiring — write yourself]**

Open `/mnt/c/90_day_python_de_plan/airflow/dags/dag_dbt_pipeline.py` and add:

```python
# Add to imports at top:
import os

GX_DIR = "/mnt/c/90_day_python_de_plan/sprint-09/day-57/gx_project/great_expectations"

# Add this function before the DAG definition:
def run_gx_checkpoint(**context) -> None:
    """
    Run GX checkpoint — validates all 3 expectation suites.
    Raises ValueError if any expectation fails.
    Airflow catches the exception → task marked FAILED
    → on_failure_callback fires → audit log entry written.
    """
    import great_expectations as gx

    # Set Windows IP for WSL2 → PostgreSQL connection
    import subprocess
    ip = subprocess.run(
        ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
        capture_output=True, text=True,
    ).stdout.strip()

    # Patch the datasource connection string if needed
    # (GX reads from great_expectations.yml — update host if 127.0.0.1)
    gx_context = gx.data_context.DataContext(context_root_dir=GX_DIR)

    print("Running GX checkpoint: ecommerce_data_quality")
    result = gx_context.run_checkpoint(
        checkpoint_name="ecommerce_data_quality"
    )

    # Log per-suite results to task logs
    for key, val in result.run_results.items():
        vr    = val["validation_result"]
        suite = vr.meta["expectation_suite_name"]
        stats = vr.statistics
        status = "✅" if vr.success else "❌"
        print(
            f"  {status} {suite}: "
            f"{stats['successful_expectations']}/{stats['evaluated_expectations']} "
            f"({stats['success_percent']:.0f}%)"
        )

    # Push summary to XCom
    context["ti"].xcom_push(key="gx_success", value=result.success)
    context["ti"].xcom_push(key="gx_suites_run", value=3)

    if not result.success:
        raise ValueError(
            "GX checkpoint FAILED — data quality issues detected. "
            "Check Data Docs for details."
        )

    print(f"GX checkpoint passed: 29/29 expectations")
```

**Wire the task — WRITE THIS YOURSELF:**

```python
# YOUR TASK: Add the GX task to the DAG and chain it after dbt_test

# Inside the DAG context (with DAG(...) as dag:), add:
# task_gx = PythonOperator(
#     task_id="run_gx_checkpoint",
#     python_callable=run_gx_checkpoint,
#     trigger_rule="all_success",   # only run if dbt_test passed
# )

# Update dependency chain:
# task_dbt_run >> task_dbt_test >> task_dbt_snapshot >> task_gx >> task_log

# YOUR CODE HERE
```

---

### EXERCISE 3 — Handle WSL2 Connection Issue (Block A/B)

The GX datasource was configured with `host=127.0.0.1` on Windows.
From WSL2, this needs the Windows IP. Fix by updating `great_expectations.yml`:

```bash
# Check current datasource config
grep -A5 "connection_string" \
    /mnt/c/90_day_python_de_plan/sprint-09/day-57/gx_project/great_expectations/great_expectations.yml

# Get Windows IP
WINDOWS_IP=$(ip route | grep default | awk '{print $3}')
echo "Windows IP: $WINDOWS_IP"

# Update connection string in great_expectations.yml
# Change 127.0.0.1 to $WINDOWS_IP in the connection_string line
sed -i "s|@127.0.0.1:5432|@${WINDOWS_IP}:5432|g" \
    /mnt/c/90_day_python_de_plan/sprint-09/day-57/gx_project/great_expectations/great_expectations.yml

# Verify
grep "connection_string" \
    /mnt/c/90_day_python_de_plan/sprint-09/day-57/gx_project/great_expectations/great_expectations.yml
```

---

### EXERCISE 4 — Trigger + Verify (Block C)

```bash
# WSL2 — trigger the dbt pipeline
airflow dags trigger dag_dbt_pipeline

# Watch tasks
sleep 60
airflow tasks states-for-dag-run dag_dbt_pipeline \
    $(airflow dags list-runs -d dag_dbt_pipeline --output json 2>/dev/null | \
      python3 -c "import json,sys; runs=json.load(sys.stdin); print(runs[0]['run_id'])")

# Expected task states:
# dbt_run           success
# dbt_test          success
# dbt_snapshot      success
# run_gx_checkpoint success   ← new task
# log_dbt_run       success
```

---

### EXERCISE 5 — Simulate GX Failure (Block D)

To verify the failure path works, temporarily break one expectation:

```python
# In customer_ltv_expectations.py — change an expectation to impossible values:
validator.expect_table_row_count_to_be_between(
    min_value=999_999,   # impossible — only 96k rows
    max_value=1_000_000,
)
# Save suite, then trigger dag_dbt_pipeline
# Expected: run_gx_checkpoint task FAILS → on_failure_callback logs to etl_audit_log
```

After verifying, restore the correct expectation.

---

### EXERCISE 6 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 60 --sprint 9 ^
    --message "Airflow+GX: run_gx_checkpoint task in dag_dbt_pipeline, failure raises ValueError, audit log" ^
    --merge
```

---

## ✅ DAY 60 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | GX installed in WSL2 airflow-venv | [ ] |
| 2 | `run_checkpoint.py` works from WSL2 | [ ] |
| 3 | `run_gx_checkpoint` function added to dag_dbt_pipeline.py | [ ] |
| 4 | **GX task wired: dbt_test → run_gx_checkpoint → log_dbt_run** | [ ] |
| 5 | ValueError raised on GX failure | [ ] |
| 6 | Airflow task shows success on checkpoint pass | [ ] |
| 7 | Failure simulation: task FAILS as expected | [ ] |
| 8 | One clean `[DAY-060][S09]` commit | [ ] |

---

## 🔜 PREVIEW: DAY 61

**Topic:** Custom GX expectations  
**What you'll do:** Write a custom expectation class that checks
business rules too complex for built-in expectations — e.g.
"on_time_rate must be higher for sellers with more than 100 orders"
or "delivery_days cannot exceed estimated_days by more than 30 days."

---

*Day 60 | Sprint 09 | EP-12 | TASK-060*
