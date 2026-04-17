**✅ Perfect!**  

Your database is **already fully populated** with the correct Pagila (DVD Rental) data:

- `film`: 1,000 rows  
- `rental`: 16,044 rows  
- `payment`: 14,596 rows  

This is exactly the standard dataset we use for the entire program.

**No need to restore Pagila again.** We can skip that step completely.

---


```markdown
# 📅 DAY 08 — Sprint 02 | Full DVD Rental Dataset + First ETL Pipeline
## Schema Validation + Production-Style Data Pipeline


## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-08: Work with Full DVD Rental Dataset                     |
| Task ID         | TASK-008                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 3                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | dvdrental, etl, pipeline, day-08                             |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-08-full-dataset`            |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-008]`                                |
| Folder        | `sprint-02/day-08/`                        |

**Create branch:**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-08-full-dataset
```

---

## 🎯 OBJECTIVES (2 hrs)

Since your `dvdrental` database is already fully populated, today we will:

1. Validate the full dataset (row counts, constraints, data quality)
2. Enhance `db_explorer.py` for production use
3. Build your **first real ETL pipeline script**
4. Export clean analytical datasets
5. Push everything cleanly to git

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                              |
|-------|----------|---------------------------------------|
| A     | 15 min   | Branch + retrospective                |
| B     | 30 min   | Dataset validation                    |
| C     | 45 min   | Build first ETL pipeline              |
| D     | 15 min   | Export + verification                 |
| E     | 15 min   | Git commit + JIRA update              |

---

## 📝 EXERCISES

### EXERCISE 1 — Quick Dataset Validation (Block B)

Create `sprint-02/day-08/validate_dataset.py`:

```python
#!/usr/bin/env python3
from db_utils import execute_scalar
import sys

print("🔍 DVD Rental Dataset Validation")
print("="*50)

tables = ["film", "actor", "customer", "rental", "payment", "inventory", "store", "category"]

for table in tables:
    count = execute_scalar(f"SELECT COUNT(*) FROM {table}")
    print(f"{' '*4}{table:<12} → {count:,} rows")

print("\n✅ Dataset looks complete!")
```

Run it:
```bash
python sprint-02/day-08/validate_dataset.py
```

### EXERCISE 2 — First ETL Pipeline (Block C)

Create `sprint-02/day-08/etl_pipeline.py`:

```python
#!/usr/bin/env python3
"""
First real ETL pipeline — Day 08
"""
import pandas as pd
from db_utils import get_engine
from logger import get_pipeline_logger
from datetime import datetime

logger = get_pipeline_logger("etl_day08")

def main():
    logger.info("Starting ETL Pipeline — Day 08")
    engine = get_engine()

    # 1. Extract + Transform
    df = pd.read_sql("""
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            COUNT(r.rental_id) AS total_rentals,
            ROUND(SUM(p.amount)::numeric, 2) AS total_spend,
            MAX(r.rental_date) AS last_rental_date
        FROM customer c
        LEFT JOIN rental r ON c.customer_id = r.customer_id
        LEFT JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        ORDER BY total_spend DESC
    """, engine)

    logger.info("Transformed customer analytics — %d rows", len(df))

    # 2. Load back to PostgreSQL
    df.to_sql("analytics_customer_lifetime", engine, if_exists="replace", index=False)
    logger.info("Loaded analytics_customer_lifetime table")

    # 3. Export CSV
    df.to_csv("sprint-02/day-08/output/customer_lifetime_value.csv", index=False)
    logger.info("Exported CSV")

    logger.info("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()
```

Run it:
```bash
mkdir -p sprint-02/day-08/output
python sprint-02/day-08/etl_pipeline.py
```

---

## ✅ DAY 08 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Branch created                                    | [ ✅ ]   |
| 2 | Dataset validation passed                         | [ ✅ ]   |
| 3 | `etl_pipeline.py` runs successfully               | [ ✅ ]   |
| 4 | `analytics_customer_lifetime` table created       | [ ✅ ]   |
| 5 | CSV exported                                      | [ ✅ ]   |
| 6 | One clean commit + merge                          | [ ✅ ]   |

---
