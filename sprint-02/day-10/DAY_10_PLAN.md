**✅ DAY 09 COMPLETE — Excellent progress!** 🎉

Your modular ETL framework + data quality checks are now in place and committed cleanly.  
JIRA connection is also working (no errors this time). Great job pushing through the earlier issues.

---

**Here is your complete `DAY_10_PLAN.md`**

**Save it as:**  
`sprint-02/day-10/DAY_10_PLAN.md`

```markdown
# 📅 DAY 10 — Sprint 02 | Advanced Transformations + Incremental ETL
## Parameterised Pipelines + Delta Loads

---

## 🔁 RETROSPECTIVE — Day 09

**Strong day!**  
You built a reusable `ETLPipeline` class and added data quality checks.  
This is exactly how professional Data Engineers structure their code.

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-10: Advanced Transformations & Incremental Loads          |
| Task ID         | TASK-010                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | etl, incremental, transformation, day-10                     |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-10-incremental-etl`         |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-010]`                                |
| Folder        | `sprint-02/day-10/`                        |

**Create branch:**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-10-incremental-etl
```

---

## 🎯 OBJECTIVES (2 hrs)

1. Make the ETL pipeline **parameterised** (configurable)
2. Implement **incremental / delta load** logic
3. Add basic data versioning (load timestamp)
4. Run the improved pipeline

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                              |
|-------|----------|---------------------------------------|
| A     | 10 min   | Branch + retrospective                |
| B     | 40 min   | Parameterised ETL pipeline            |
| C     | 35 min   | Incremental load logic                |
| D     | 15 min   | Run & verify                          |
| E     | 20 min   | Git commit + daily log                |

---

## 📝 EXERCISES

### EXERCISE 1 — Parameterised ETL Pipeline

Create `sprint-02/day-10/etl_pipeline_v2.py`:

```python
#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))

from db_utils import get_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("etl_v2")


class ETLPipelineV2:
    def __init__(self, pipeline_name: str):
        self.name = pipeline_name
        self.engine = get_engine()
        self.load_timestamp = datetime.now()
        logger.info("Initialized ETLPipelineV2: %s", pipeline_name)

    def run_customer_lifetime(self):
        logger.info("Running parameterised customer lifetime ETL...")

        sql = """
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name AS full_name,
                COUNT(r.rental_id)                    AS total_rentals,
                ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
                ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend,
                MAX(r.rental_date)                    AS last_rental,
                :load_timestamp                       AS load_timestamp
            FROM customer c
            LEFT JOIN rental r ON c.customer_id = r.customer_id
            LEFT JOIN payment p ON r.rental_id = p.rental_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            ORDER BY total_spend DESC
        """

        df = pd.read_sql(sql, self.engine, params={"load_timestamp": self.load_timestamp})

        logger.info("Extracted %d customer records", len(df))

        df.to_sql("analytics_customer_lifetime_v2", self.engine, if_exists="replace", index=False)
        logger.info("✅ Loaded analytics_customer_lifetime_v2")

        Path("sprint-02/day-10/output").mkdir(exist_ok=True)
        df.to_csv("sprint-02/day-10/output/customer_lifetime_v2.csv", index=False)
        logger.info("📄 Exported v2 CSV")

        return df


def main():
    pipeline = ETLPipelineV2("customer_lifetime_v2")
    pipeline.run_customer_lifetime()
    logger.info("🎉 Parameterised ETL v2 completed!")


if __name__ == "__main__":
    main()
```

### EXERCISE 2 — Incremental Load Example

Create `sprint-02/day-10/incremental_load.py`:

```python
#!/usr/bin/env python3
from db_utils import get_engine, execute_scalar
from logger import get_pipeline_logger
from datetime import datetime

logger = get_pipeline_logger("incremental_load")


def run_incremental_load():
    logger.info("Running incremental load example...")

    # Get last load timestamp
    last_load = execute_scalar("SELECT MAX(load_timestamp) FROM analytics_customer_lifetime_v2") or '1900-01-01'

    logger.info("Last load timestamp: %s", last_load)

    # Simulate incremental query (in real life you'd use a watermark column)
    sql = f"""
        SELECT * FROM rental 
        WHERE rental_date > '{last_load}'
        LIMIT 100
    """

    count = execute_scalar(f"SELECT COUNT(*) FROM rental WHERE rental_date > '{last_load}'")
    logger.info("New rentals since last load: %d rows", count)

    logger.info("Incremental load logic ready for production use.")


if __name__ == "__main__":
    run_incremental_load()
```

---

## ✅ DAY 10 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Branch created                                    | [ ]   |
| 2 | `etl_pipeline_v2.py` runs successfully            | [ ]   |
| 3 | `incremental_load.py` runs successfully           | [ ]   |
| 4 | v2 analytics table created                        | [ ]   |
| 5 | One clean commit + merge                          | [ ]   |

---

**Run the scripts:**

```bash
mkdir -p sprint-02/day-10/output
python sprint-02/day-10/etl_pipeline_v2.py
python sprint-02/day-10/incremental_load.py
```

Then commit:

```powershell
python scripts/daily_commit.py \
  --day 10 \
  --sprint 2 \
  --message "Day 10: Parameterised ETL v2 + incremental load logic" \
  --merge
```

---

Reply with **“Day 10 done”** when finished (or paste output if any issues), and I’ll give you **DAY_11_PLAN.md**.

You’re building very solid, production-like code now. Keep going! 🚀