Here is your complete DAY_09_PLAN.md

Save it as:
sprint-02/day-09/DAY_09_PLAN.md

# 📅 DAY 09 — Sprint 02 | Modular ETL Framework + Data Quality Checks
## Building Reusable Production Pipelines


## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-09: Modular ETL Framework + Data Quality                  |
| Task ID         | TASK-009                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | etl, modular, data-quality, day-09                           |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-09-modular-etl`             |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-009]`                                |
| Folder        | `sprint-02/day-09/`                        |

**Create branch:**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-09-modular-etl
🎯 OBJECTIVES (2 hrs)
Refactor yesterday’s ETL into a reusable class-based framework
Add a dedicated data quality module
Run both together as a mini pipeline
Commit cleanly
📝 EXERCISES
EXERCISE 1 — Modular ETL Framework
Create sprint-02/day-09/etl_framework.py:

#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))

from db_utils import get_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("etl_framework")


class ETLPipeline:
    """Reusable ETL pipeline class for the rest of the program."""

    def __init__(self, pipeline_name: str):
        self.name = pipeline_name
        self.engine = get_engine()
        logger.info("Initialized ETL Pipeline: %s", pipeline_name)

    def extract(self, sql: str) -> pd.DataFrame:
        logger.info("Extracting data with query...")
        df = pd.read_sql(sql, self.engine)
        logger.info("Extracted %d rows", len(df))
        return df

    def load(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
        logger.info("Loading data into table: %s", table_name)
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        logger.info("✅ Loaded %d rows into %s", len(df), table_name)

    def export_csv(self, df: pd.DataFrame, filename: str):
        Path("sprint-02/day-09/output").mkdir(exist_ok=True)
        path = f"sprint-02/day-09/output/{filename}"
        df.to_csv(path, index=False)
        logger.info("📄 Exported CSV → %s", path)


def main():
    pipeline = ETLPipeline("customer_lifetime")

    sql = """
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS full_name,
            COUNT(r.rental_id)                    AS total_rentals,
            ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
            ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend_per_rental
        FROM customer c
        LEFT JOIN rental r ON c.customer_id = r.customer_id
        LEFT JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        ORDER BY total_spend DESC
    """

    df = pipeline.extract(sql)
    pipeline.load(df, "analytics_customer_lifetime")
    pipeline.export_csv(df, "customer_lifetime_day09.csv")

    logger.info("🎉 Modular ETL completed successfully!")


if __name__ == "__main__":
    main()
EXERCISE 2 — Data Quality Module
Create sprint-02/day-09/data_quality.py:

#!/usr/bin/env python3
from db_utils import execute_scalar
from logger import get_pipeline_logger

logger = get_pipeline_logger("data_quality")


def run_quality_checks():
    logger.info("Running data quality checks on dvdrental...")

    checks = [
        ("film row count", "SELECT COUNT(*) FROM film", 1000),
        ("rental row count", "SELECT COUNT(*) FROM rental", 16044),
        ("payment row count", "SELECT COUNT(*) FROM payment", 14596),
        ("No negative amounts", "SELECT COUNT(*) FROM payment WHERE amount < 0", 0),
        ("No future rental dates", "SELECT COUNT(*) FROM rental WHERE rental_date > NOW()", 0),
    ]

    for name, sql, expected in checks:
        result = execute_scalar(sql)
        status = "✅ PASS" if result == expected else f"❌ FAIL (got {result})"
        logger.info(f"{status} | {name}")

    logger.info("Data quality checks completed.")


if __name__ == "__main__":
    run_quality_checks()
✅ DAY 09 COMPLETION CHECKLIST
#	Task	Done?
1	Branch created	[ ]
2	etl_framework.py runs successfully	[ ]
3	data_quality.py runs successfully	[ ]
4	New analytics table created	[ ]
5	CSV exported in output/	[ ]
6	One clean commit + merge	[ ]
Run the scripts:

mkdir -p sprint-02/day-09/output

python sprint-02/day-09/etl_framework.py
python sprint-02/day-09/data_quality.py
Then commit:

python scripts/daily_commit.py \
  --day 9 \
  --sprint 2 \
  --message "Day 09: Modular ETL framework + data quality checks" \
  --merge
Reply with “Day 09 done” (or paste any output) when finished, and I will immediately give you DAY_10_PLAN.md.

You're now building professional, reusable Data Engineering code. Keep the momentum! 🚀

✅ Got it!

Yes, we can (and should) add Git History Cleanup as a proper task in Day 09. Duplicate commits are a common beginner issue when running daily_commit.py multiple times, and learning how to clean them safely is excellent practice.

Updated DAY_09_PLAN.md (with Git Cleanup Task)
Save this as sprint-02/day-09/DAY_09_PLAN.md

# 📅 DAY 09 — Sprint 02 | Modular ETL Framework + Data Quality + Git Cleanup
## Reusable Pipelines + Professional Git Hygiene

---

## 🔁 RETROSPECTIVE — Day 08

**Great job!**  
You validated the full dataset and built your first real ETL pipeline.

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-09: Modular ETL + Data Quality + Git Hygiene             |
| Task ID         | TASK-009                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 4                                                            |
| Priority        | HIGH                                                         |
| Labels          | etl, modular, data-quality, git-hygiene, day-09              |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-09-modular-etl`             |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-009]`                                |
| Folder        | `sprint-02/day-09/`                        |

**Create branch:**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-09-modular-etl
🎯 OBJECTIVES (2 hrs)
Build a reusable ETL framework (class-based)
Add a data quality module
Clean duplicate commits from Git history (new task)
Run the improved pipeline
📝 EXERCISES
EXERCISE 1 — Modular ETL Framework
(Use the version I gave you earlier — etl_framework.py)

EXERCISE 2 — Data Quality Module
(Use the version I gave you earlier — data_quality.py)

EXERCISE 3 — Git History Cleanup (New Task)
Goal: Remove duplicate commits from previous days (especially Day 05, 06, 08).

Step-by-step Safe Cleanup
cd C:\90_day_python_de_plan

# 1. Go to the feature branch you want to clean
git checkout sprint-02/day-08-full-dataset     # change to the branch with duplicates

# 2. Start interactive rebase on the last N commits
git rebase -i HEAD~10     # adjust number if needed (look at git log first)

# 3. In the editor that opens:
#    - Change "pick" to "squash" or "fixup" for duplicate commits
#    - Keep only one "pick" for the main commit
#    - Save and close

# 4. Force push the cleaned branch (safe because it's your feature branch)
git push origin sprint-02/day-08-full-dataset --force-with-lease
Important Rules:

Never do this on develop or main.
Only clean unmerged feature branches.
After cleaning, you can re-merge if needed.
Run this cleanup now on any branch that has duplicates.

✅ DAY 09 COMPLETION CHECKLIST
#	Task	Done?
1	Branch created	[ ]
2	etl_framework.py runs successfully	[ ]
3	data_quality.py runs successfully	[ ]
4	Git history cleaned (duplicates removed)	[ ]
5	One clean commit + merge	[ ]
Next Steps:

Replace the two files with the versions I gave you earlier.
Run the cleanup steps above on the Day 08 branch (or any branch with duplicates).
Run the two new scripts.
Commit with:
python scripts/daily_commit.py \
  --day 9 \
  --sprint 2 \
  --message "Day 09: Modular ETL framework + data quality checks + Git history cleanup" \
  --merge
Reply with “Day 09 done” (or paste output) when finished, and I’ll send you DAY_10_PLAN.md.

Would you like me to also give you a one-command script to help with future Git cleanups? Just say yes.

You’re doing really well — keep going! 🚀