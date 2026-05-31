# E-Commerce Analytics Platform
### Python Data Engineering Capstone — 90-Day DE Journey

End-to-end analytics platform built on 100,000 real Brazilian e-commerce orders
(Olist dataset, 2016-2018). Demonstrates the full DE stack: ETL pipelines →
SQL transforms → ML models → Airflow orchestration → Streamlit dashboard.

---
## 🚀 Quick Start

### Prerequisites
- Python 3.12, PostgreSQL 17
- Virtual environment activated
- `.env` configured (see `.env.example`)

### Run the Dashboard
```bash
cd C:\90_day_python_de_plan
capstone\dashboard\run_dashboard.bat
# Opens: http://localhost:8502
```

### Run the Full Pipeline
```bash
# Manual trigger (or @weekly via Airflow)
# In WSL2:
airflow dags trigger dag_ecommerce_etl
```

---

## 📊 Dashboard Pages

| Page | Key Metrics | Tech |
|------|------------|------|
| Overview | $16M revenue, 96k customers, 4.15⭐ avg review | Plotly, Streamlit |
| Orders | 8.11% late rate, delivery time analysis | Plotly histogram + box |
| Customers | 3% repeat rate, $167 avg LTV, churn risk | Plotly scatter |
| Sellers | 3,095 sellers, 85.28% on-time rate | Plotly scatter + table |
| ML Insights | Churn predictions, delay probability | Plotly histogram |

---

## 🏗️ Architecture

```
Raw CSVs (Kaggle Olist)
    │   8 tables, 550k rows
    ▼
PostgreSQL — raw schema
    │   load_raw_data.py
    ▼
PostgreSQL — analytics schema         ← analytics_etl.py
    ├── customer_ltv      96,218 rows
    ├── order_metrics     96,588 rows
    ├── seller_performance 3,095 rows
    ├── product_analytics     71 rows
    └── monthly_revenue       22 rows
    │
    ├── churn_model.py  → ml.churn_predictions (96,218 rows)
    └── delay_model.py  → ml.delay_predictions (96,588 rows)
    │
    ▼
Airflow DAG: dag_ecommerce_etl (@weekly)
    │   run_analytics_etl → [run_churn_model ‖ run_delay_model] → log_pipeline_run
    ▼
Streamlit Dashboard (port 8502)
    └── 5 pages with live PostgreSQL data
```

---

## 📁 Project Structure

```
capstone/
├── data/raw/           ← Olist CSV files (not committed — 150MB)
├── etl/
│   ├── load_raw_data.py   ← CSV → PostgreSQL raw schema
│   └── analytics_etl.py   ← raw → 5 analytics tables
├── ml/
│   ├── churn_model.py     ← Customer churn prediction
│   ├── delay_model.py     ← Delivery delay prediction
│   └── models/            ← Saved pipelines (.pkl)
├── dashboard/
│   ├── app.py             ← Streamlit entry point
│   ├── db.py              ← Cached query functions
│   └── pages/             ← 5 dashboard pages
└── docs/
    ├── ARCHITECTURE.md    ← System design
    └── DATA_QUALITY.md    ← Known issues
```

---

## 🤖 ML Models

| Model | Target | Algorithm | CV F1 | Notes |
|-------|--------|-----------|-------|-------|
| Churn | Single-purchase customer | RF + SMOTE | ~0.22 | Limited by feature availability; total_orders excluded (leakage) |
| Delay | Order delivered late | RF + SMOTE | ~0.15 | Weak features; seller history would improve significantly |

Both models use `imbalanced-learn` ImbPipeline (SMOTE → StandardScaler → RandomForest)
and are saved as single `.pkl` artifacts for reproducible inference.

---

## 🔑 Key Business Findings

1. **Late deliveries cost 1.72 review points**: On-time = 4.29⭐, Late = 2.57⭐
2. **97% of customers buy only once**: Typical marketplace behaviour; retention is the core business challenge
3. **8.11% of orders are late**: 7,837 orders affected; concentrated in specific seller-state combinations
4. **Top category**: [fill in from your product_analytics query]
5. **Best sellers**: 85.28% average on-time rate; high variance between top and bottom performers

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Database | PostgreSQL 17 |
| ETL | Python, pandas, SQLAlchemy |
| Orchestration | Apache Airflow 2.9.3 (WSL2) |
| ML | scikit-learn, imbalanced-learn |
| Visualisation | Plotly, Streamlit |
| Version Control | Git (GitHub) |

---

## 📝 Data Source

Olist Brazilian E-Commerce Public Dataset  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce  
License: CC BY-NC-SA 4.0

---