# 🎬 DVD Rental Analytics Dashboard

Interactive analytics dashboard built with Streamlit, Plotly, and PostgreSQL.

## Screenshots

| Overview | Customers | Films |
|----------|-----------|-------|
| ![Overview](../../sprint-05/day-31/output/Analytics-Overview.pdf) | ![Customers](../../sprint-05/day-31/output/Customer-Analytics.pdf) | ![Films](../../sprint-05/day-31/output/Films_Analytics.pdf) |

## Quick Start
- double click on shortcut of bat file on desktop
---
### Prerequisites
- Python 3.12+
- PostgreSQL running locally (dvdrental database)
- Virtual environment activated

### Run
```bash
cd C:\90_day_python_de_plan
.venv\Scripts\activate
streamlit run sprint-05/day-31/app.py
```
Open: http://localhost:8501
---
### Pages
| Page | Description |
|------|-------------|
| 📊 Overview | KPI cards, pipeline status, revenue trend |
| 👥 Customers | Segment analysis, spend distribution, filters |
| 🎬 Films | Value tier analysis, rating distribution, search |
---
### Data Sources (PostgreSQL dvdrental)
| Table | Description |
|-------|-------------|
| analytics_customer_airflow | Customer segments + spend metrics |
| analytics_film_airflow | Film value tiers |
| analytics_film_value_score | Film value scores + rental counts |
| analytics_monthly_enriched | Monthly revenue + MoM growth |
| etl_audit_log | Pipeline run history |
---
### Features
- Live PostgreSQL connection
- 5-minute data cache (configurable)
- Manual refresh button
- Interactive Plotly charts
- CSV download on all pages
- Title search on films page
---
## Architecture
```
PostgreSQL (Windows) ← Airflow ETL DAGs (WSL2)
       ↓
   db.py (@st.cache_data)
       ↓
Streamlit app (Windows)
  ├── pages/overview.py
  ├── pages/customers.py
  └── pages/films.py
```
```