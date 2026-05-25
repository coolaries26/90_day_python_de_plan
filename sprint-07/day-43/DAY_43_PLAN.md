# 📅 DAY 43 — Sprint 07 | Capstone Design + Architecture
## E-Commerce Orders Capstone — Design, Dataset Setup, Project Scaffold

---

## 🔁 SPRINT 06 CLOSE — Confirmed

```
sprint-06-complete tag: ✅
Score: 93/100
All 6 sprint tags: ✅
```

### Fix Before Starting
```bash
# JIRA bug: jira_client called as class not instance
# Open scripts/daily_commit.py, find JIRA instantiation
# Verify: jira_client = JiraClient()  ← parentheses required
# NOT:    jira_client = JiraClient

cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-07/day-43-capstone-design
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-10: Capstone & Job Readiness                              |
| Story           | ST-43: Capstone Design + Dataset Setup                       |
| Task ID         | TASK-043                                                     |
| Sprint          | Sprint 07 (Days 43–48)                                       |
| Story Points    | 3                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | capstone, ecommerce, design, architecture, day-43            |
| Acceptance Criteria | Dataset loaded to PostgreSQL; architecture diagram drawn; project scaffold created; ETL design documented |

---

## 📚 DATASET: Brazilian E-Commerce (Olist)

### What It Is

```
100,000 real orders from Olist (Brazilian e-commerce marketplace)
Period: 2016-2018
Tables: 8
Source: Kaggle — https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Tables:
  olist_orders_dataset.csv              → 99,441 orders with status + timestamps
  olist_order_items_dataset.csv         → items within each order
  olist_order_payments_dataset.csv      → payment type + value
  olist_order_reviews_dataset.csv       → review score + comments
  olist_customers_dataset.csv           → customer location
  olist_sellers_dataset.csv             → seller location
  olist_products_dataset.csv            → product category + dimensions
  olist_order_customer_dataset.csv      → customer ↔ order link (some versions merged)
  product_category_name_translation.csv → Portuguese → English category names
```

### Why This Dataset Is Recruiter-Gold

```
Interviewers ask about this exact dataset at:
  - Amazon, Shopify, Mercado Libre, Zalando (DE roles)
  - Any startup with an e-commerce product

You can demo:
  "I built an end-to-end pipeline: 
   100k orders → PostgreSQL → ETL → ML churn prediction → 
   automated retraining via Airflow → live Streamlit dashboard"
```

### ERD (Key Relationships)

```
customers ──< orders ──< order_items ──< products
                │
                ├──< order_payments
                ├──< order_reviews
                └──< sellers (via order_items)
```

---

## 🎯 CAPSTONE OBJECTIVES (Days 43–48)

```
Day 43  Design + dataset setup + project scaffold
Day 44  ETL pipelines: raw CSV → PostgreSQL → analytics tables
Day 45  ML: customer churn + delivery delay prediction
Day 46  Airflow: orchestrate full pipeline
Day 47  Streamlit: dashboard + model insights
Day 48  Code review + README + final demo
```

### What You'll Build

```
                    ┌─────────────────────────────────┐
                    │     E-COMMERCE DATA PLATFORM     │
                    └─────────────────────────────────┘

Raw CSVs (Kaggle)
    │
    ▼  [ETL Day 44]
PostgreSQL (ecommerce_db)
  ├── raw.*          ← 8 source tables (direct CSV load)
  └── analytics.*    ← 5 derived tables
       ├── customer_ltv        ← Lifetime Value per customer
       ├── order_metrics       ← Delivery time, review scores
       ├── seller_performance  ← Revenue, rating per seller
       ├── product_analytics   ← Category revenue, return rates
       └── monthly_revenue     ← Time series
    │
    ▼  [ML Day 45]
ML Models
  ├── churn_model.pkl          ← Will customer buy again?
  └── delay_model.pkl          ← Will order be late?
    │
    ▼  [Airflow Day 46]
Scheduled Pipelines
  ├── dag_ecommerce_etl        ← Daily refresh
  ├── dag_ml_retrain           ← Weekly model retrain
  └── dag_dashboard_refresh    ← Chart regeneration
    │
    ▼  [Streamlit Day 47]
Dashboard
  ├── Overview (KPIs)
  ├── Orders    (trends, delays)
  ├── Customers (LTV, segments)
  ├── Sellers   (performance)
  └── ML Insights (churn risk, delay prediction)
```

---

## 🎯 TODAY'S OBJECTIVES

1. Download Olist dataset from Kaggle
2. Create `ecommerce_db` PostgreSQL database + `appuser` permissions
3. Load all 8 CSV files into `raw` schema
4. Create project scaffold (`capstone/` folder structure)
5. Write architecture diagram + design document
6. Push clean `[DAY-043][S07]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 20 min   | Download dataset + create DB                       |
| B     | 30 min   | `load_raw_data.py` — CSV → PostgreSQL              |
| C     | 20 min   | Project scaffold                                   |
| D     | 30 min   | `ARCHITECTURE.md` + design decisions               |
| E     | 20 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Download Dataset + Create DB (Block A)

**Download Olist dataset:**
```bash
# Option A: Kaggle CLI (recommended)
pip install kaggle
# Place your kaggle.json API key at C:\Users\Lenovo\.kaggle\kaggle.json
kaggle datasets download -d olistbr/brazilian-ecommerce
# Extract to: C:\90_day_python_de_plan\capstone\data\raw\

# Option B: Manual download
# Go to: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Download ZIP → extract to capstone/data/raw/
```

**Create `ecommerce_db` in PostgreSQL:**
```sql
-- Run as postgres superuser
psql -h 127.0.0.1 -U postgres -W << 'EOF'

CREATE DATABASE ecommerce_db
    WITH OWNER = postgres
    ENCODING = 'UTF8'
    TEMPLATE = template0;

-- Grant appuser access (reuse existing appuser from dvdrental)
GRANT CONNECT ON DATABASE ecommerce_db TO appuser;

\c ecommerce_db

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ml;

-- Grant appuser schema permissions
GRANT USAGE ON SCHEMA raw, analytics, ml TO appuser;
GRANT CREATE ON SCHEMA raw, analytics, ml TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, analytics, ml
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, analytics, ml
    GRANT USAGE, SELECT ON SEQUENCES TO appuser;

EOF
```

**Update `.env` — add ecommerce DB:**
```env
# Add to .env:
ECOMMERCE_DB_HOST=127.0.0.1
ECOMMERCE_DB_PORT=5432
ECOMMERCE_DB_NAME=ecommerce_db
ECOMMERCE_DB_USER=appuser
ECOMMERCE_DB_PASSWORD=AppUser@2024!
```

---

### EXERCISE 2 — Project Scaffold (Block C)
**[Full steps — folder structure for capstone]**

```bash
mkdir -p capstone/{data/{raw,processed},etl,ml,dags,dashboard/pages,tests,docs}
touch capstone/__init__.py
touch capstone/etl/__init__.py
touch capstone/ml/__init__.py
touch capstone/data/raw/.gitkeep
touch capstone/data/processed/.gitkeep
```

**Create `capstone/db.py` — ecommerce DB connection:**
```python
#!/usr/bin/env python3
"""
capstone/db.py — Database connection for ecommerce_db
Mirrors sprint-01/day-02/db_utils.py but for ecommerce_db.
"""
import os
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

try:
    from dotenv import load_dotenv
    for candidate in [Path(__file__).parent.parent, Path.home() / "python-de-journey"]:
        env = candidate / ".env"
        if env.is_file():
            load_dotenv(dotenv_path=env, override=False)
            break
except ImportError:
    pass

_engine: Engine | None = None

def get_ecommerce_engine() -> Engine:
    global _engine
    if _engine is None:
        pwd = quote_plus(os.environ.get("ECOMMERCE_DB_PASSWORD", ""))
        url = (
            f"postgresql+psycopg2://"
            f"{os.environ.get('ECOMMERCE_DB_USER', 'appuser')}:{pwd}"
            f"@{os.environ.get('ECOMMERCE_DB_HOST', '127.0.0.1')}"
            f":{os.environ.get('ECOMMERCE_DB_PORT', 5432)}"
            f"/{os.environ.get('ECOMMERCE_DB_NAME', 'ecommerce_db')}"
        )
        _engine = create_engine(
            url,
            pool_size=5,
            max_overflow=2,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,
        )
    return _engine

def dispose_ecommerce_engine() -> None:
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None
```

---

### EXERCISE 3 — load_raw_data.py (Block B)
**[Q1 provided. Q2 write yourself]**

Create `capstone/etl/load_raw_data.py`:

```python
#!/usr/bin/env python3
"""
load_raw_data.py — Capstone Day 43
====================================
Loads all 8 Olist CSV files into PostgreSQL raw schema.
Run once to populate raw tables.
Run: python capstone/etl/load_raw_data.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("load_raw_data")

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Mapping: CSV filename → target table name in raw schema
CSV_TABLE_MAP = {
    "olist_orders_dataset.csv":              "orders",
    "olist_order_items_dataset.csv":         "order_items",
    "olist_order_payments_dataset.csv":      "order_payments",
    "olist_order_reviews_dataset.csv":       "order_reviews",
    "olist_customers_dataset.csv":           "customers",
    "olist_sellers_dataset.csv":             "sellers",
    "olist_products_dataset.csv":            "products",
    "product_category_name_translation.csv": "product_category_translation",
}


# ── Q1: Load one CSV — provided ───────────────────────────────────────────
def load_csv_to_db(
    csv_path: Path,
    table_name: str,
    engine,
    schema: str = "raw",
) -> int:
    """Load a single CSV file into PostgreSQL. Returns rows loaded."""
    if not csv_path.exists():
        logger.warning(f"File not found: {csv_path.name} — skipping")
        return 0

    logger.info(f"Loading {csv_path.name}...")
    df = pd.read_csv(csv_path, low_memory=False)
    logger.info(f"  Read: {len(df):,} rows × {len(df.columns)} cols")

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info(f"  ✅ Loaded → {schema}.{table_name} ({len(df):,} rows)")
    return len(df)


# ── Q2: Load all tables — WRITE THIS YOURSELF ─────────────────────────────
def load_all_tables() -> dict[str, int]:
    """
    Q2 — YOUR TASK:
    Load all 8 CSV files using load_csv_to_db().
    Return a dict of {table_name: row_count}.

    HINTS:
    engine = get_ecommerce_engine()
    results = {}
    for csv_file, table_name in CSV_TABLE_MAP.items():
        csv_path = RAW_DATA_DIR / csv_file
        count = load_csv_to_db(csv_path, table_name, engine)
        results[table_name] = count

    logger.info(f"\nLoad Summary:")
    total = 0
    for table, count in results.items():
        logger.info(f"  raw.{table:<40} {count:>8,} rows")
        total += count
    logger.info(f"  Total rows loaded: {total:,}")

    dispose_ecommerce_engine()
    return results
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement load_all_tables")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Loading Raw Data — Day 43 Capstone")
    logger.info("=" * 52)
    logger.info(f"Data directory: {RAW_DATA_DIR}")

    results = load_all_tables()
    logger.info(f"\n✅ All tables loaded. Total: {sum(results.values()):,} rows")


if __name__ == "__main__":
    main()
```

**Expected output:**
```
raw.orders                         99,441 rows
raw.order_items                   112,650 rows
raw.order_payments                103,886 rows
raw.order_reviews                  99,224 rows
raw.customers                      99,441 rows
raw.sellers                         3,095 rows
raw.products                       32,951 rows
raw.product_category_translation       71 rows
Total rows loaded: 551,959 rows
```

---

### EXERCISE 4 — ARCHITECTURE.md (Block D)
**[Write yourself — your design document]**

Create `capstone/docs/ARCHITECTURE.md`:

```markdown
# Capstone Architecture — E-Commerce Analytics Platform

## Dataset
- Source: Olist Brazilian E-Commerce (Kaggle)
- Volume: ~100k orders, 8 tables, 550k total rows
- Period: 2016–2018

## Database Design

### raw schema — direct CSV loads (no transformation)
| Table | Rows | Key Column |
|-------|------|------------|
| orders | 99,441 | order_id |
| order_items | 112,650 | order_id, order_item_id |
| order_payments | 103,886 | order_id |
| order_reviews | 99,224 | review_id |
| customers | 99,441 | customer_id |
| sellers | 3,095 | seller_id |
| products | 32,951 | product_id |
| product_category_translation | 71 | product_category_name |

### analytics schema — derived tables (ETL output)
| Table | Description | Built from |
|-------|-------------|------------|
| customer_ltv | Customer lifetime value + churn flag | orders + payments + customers |
| order_metrics | Delivery time, review score per order | orders + reviews + order_items |
| seller_performance | Revenue, rating, on-time % per seller | order_items + orders + reviews |
| product_analytics | Revenue, volume per category | order_items + products + translation |
| monthly_revenue | Monthly revenue time series + MoM growth | orders + order_payments |

### ml schema — ML outputs
| Table | Description |
|-------|-------------|
| churn_predictions | Customer churn probability |
| delay_predictions | Order delivery delay probability |
| customer_segments | KMeans cluster assignments |

## ML Models
| Model | Target | Features | Algorithm |
|-------|--------|----------|-----------|
| Churn | Has customer ordered again? | LTV, days since last order, review score | RandomForest + SMOTE |
| Delay | Will order be delivered late? | product weight, distance, seller rating | GradientBoosting |

## Pipeline Architecture
[Add your architecture diagram or Mermaid chart here]

## Design Decisions
1. Separate raw/analytics/ml schemas for clean separation
2. appuser NOCREATEDB — uses GRANT CREATE per schema
3. All timestamps stored as TIMESTAMP (not VARCHAR)
4. English category names via JOIN to translation table
```

---

### EXERCISE 5 — Verify Raw Data Load

```bash
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
result = pd.read_sql('''
    SELECT table_name,
           pg_size_pretty(pg_total_relation_size(
               quote_ident(table_schema)||'.'||quote_ident(table_name)::text)) AS size
    FROM information_schema.tables
    WHERE table_schema = 'raw'
    ORDER BY table_name
''', engine)
print(result.to_string(index=False))
dispose_ecommerce_engine()
"
```

---

### EXERCISE 6 — Git Push

```bash
python scripts/daily_commit.py --day 43 --sprint 7 ^
    --message "Capstone: ecommerce_db setup, raw schema loaded 550k rows, architecture doc, project scaffold" ^
    --merge
```

---

## ✅ DAY 43 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | Olist dataset downloaded and extracted to capstone/data/raw/             | [ ]   |
| 2 | `ecommerce_db` created with raw/analytics/ml schemas                    | [ ]   |
| 3 | `capstone/` folder structure created                                     | [ ]   |
| 4 | `capstone/db.py` created — ecommerce engine factory                      | [ ]   |
| 5 | **`load_all_tables()` written — all 8 CSVs loaded**                      | [ ]   |
| 6 | `raw` schema has 8 tables totalling ~550k rows                           | [ ]   |
| 7 | `capstone/docs/ARCHITECTURE.md` written                                  | [ ]   |
| 8 | One clean `[DAY-043][S07]` commit via `daily_commit.py --merge`          | [ ]   |

---

## ⚠️ WATCH OUT FOR

**CSV encoding issues:**
Some Olist files have UTF-8 encoding issues on Windows.
```python
df = pd.read_csv(csv_path, low_memory=False, encoding="utf-8")
# If encoding errors: encoding="latin-1"
```

**order_reviews has HTML in comments:**
The `review_comment_message` column contains raw HTML.
This is expected — the ETL transform stage (Day 44) will clean it.
Load it as-is today, clean it tomorrow.

**product_category_name is Portuguese:**
Raw products table has Portuguese category names.
The translation table maps them to English.
Day 44 ETL will apply the JOIN.

**appuser permissions on new schemas:**
If you get "permission denied for schema raw":
```sql
-- Run as postgres:
\c ecommerce_db
GRANT CREATE ON SCHEMA raw TO appuser;
GRANT ALL ON ALL TABLES IN SCHEMA raw TO appuser;
```

---

## 🔜 PREVIEW: DAY 44

**Topic:** ETL — raw → analytics tables  
**What you'll do:** Build 5 analytics tables from the raw schema:
`customer_ltv`, `order_metrics`, `seller_performance`, `product_analytics`, `monthly_revenue`.
Each is a complex SQL transformation that would be a standard interview question.

---

*Day 43 | Sprint 07 | EP-10 | TASK-043*
