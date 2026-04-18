**✅ Conflict Resolved & Branch Created Successfully!**  

You are now on the clean new branch:  
`sprint-02/day-11-config-etl`

---

**Here is your complete `DAY_11_PLAN.md`**

**Save it as:**  
`sprint-02/day-11/DAY_11_PLAN.md`

```markdown
# 📅 DAY 11 — Sprint 02 | Config-Driven ETL Pipelines
## Production-Grade Config Management + Robust Error Handling

---

## 🔁 RETROSPECTIVE — Day 10

**Good work!**  
You now have parameterised ETL and incremental load logic.  
Today we make the pipeline truly production-ready with configuration.

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-11: Config-Driven ETL + Robust Error Handling             |
| Task ID         | TASK-011                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | etl, config, pydantic, error-handling, day-11                |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-11-config-etl`              |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-011]`                                |
| Folder        | `sprint-02/day-11/`                        |

---

## 🎯 OBJECTIVES (2 hrs)

1. Create a **centralized configuration** using Pydantic
2. Build a **config-driven ETL pipeline**
3. Add proper error handling and logging
4. Run the pipeline using configuration

---

## 📝 EXERCISES

### EXERCISE 1 — Config Module

Create folder and file:  
`sprint-02/day-11/config/etl_config.py`

```python
#!/usr/bin/env python3
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from datetime import datetime

class ETLConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Pipeline settings
    PIPELINE_NAME: str = "customer_lifetime"
    TARGET_TABLE: str = "analytics_customer_lifetime_v2"
    OUTPUT_CSV: str = "customer_lifetime_v2.csv"

    # Data quality
    MIN_ROWS_EXPECTED: int = 500

    # Runtime
    LOAD_TIMESTAMP: str = Field(default_factory=lambda: datetime.now().isoformat())

settings = ETLConfig()
```

### EXERCISE 2 — Config-Driven ETL Pipeline

Create `sprint-02/day-11/etl_pipeline_config.py`:

```python
#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))

from db_utils import get_engine
from logger import get_pipeline_logger
from config.etl_config import settings

logger = get_pipeline_logger("etl_config")


class ConfigETLPipeline:
    def __init__(self):
        self.engine = get_engine()
        logger.info("Initialized Config-Driven ETL | Pipeline: %s", settings.PIPELINE_NAME)

    def run(self):
        logger.info("Running config-driven customer lifetime ETL...")

        sql = """
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name AS full_name,
                COUNT(r.rental_id)                    AS total_rentals,
                ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
                ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend_per_rental,
                MAX(r.rental_date)                    AS last_rental,
                NOW()                                 AS load_timestamp
            FROM customer c
            LEFT JOIN rental r ON c.customer_id = r.customer_id
            LEFT JOIN payment p ON r.rental_id = p.rental_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            ORDER BY total_spend DESC
        """

        df = pd.read_sql(sql, self.engine)
        logger.info("Extracted %d rows", len(df))

        # Load using config
        df.to_sql(settings.TARGET_TABLE, self.engine, if_exists="replace", index=False)
        logger.info("✅ Loaded table: %s", settings.TARGET_TABLE)

        # Export using config
        Path("sprint-02/day-11/output").mkdir(exist_ok=True)
        df.to_csv(f"sprint-02/day-11/output/{settings.OUTPUT_CSV}", index=False)
        logger.info("📄 Exported CSV → %s", settings.OUTPUT_CSV)

        logger.info("🎉 Config-Driven ETL completed successfully!")


if __name__ == "__main__":
    pipeline = ConfigETLPipeline()
    pipeline.run()
```

---

## ✅ DAY 11 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Branch created                                    | [ ]   |
| 2 | Config module created                             | [ ]   |
| 3 | Config-driven pipeline runs successfully          | [ ]   |
| 4 | One clean commit + merge                          | [ ]   |

---

**Run the pipeline:**

```bash
mkdir -p sprint-02/day-11/output
python sprint-02/day-11/etl_pipeline_config.py
```

Then commit:

```powershell
python scripts/daily_commit.py \
  --day 11 \
  --sprint 2 \
  --message "Day 11: Config-driven ETL pipeline using Pydantic" \
  --merge
```

---

Reply with **“Day 11 done”** when finished (or paste output if any issues), and I will immediately give you **DAY_12_PLAN.md**.

You're building very clean, maintainable Data Engineering code now. Keep going! 🚀