# 🎬 DVD Rental Analytics Dashboard

**A modern, interactive Streamlit analytics dashboard** for the classic **dvdrental** PostgreSQL database.

Built as part of the **Python Data Engineering Journey** (Sprint 5 — Days 31 & 32).  
The dashboard provides real-time business intelligence on customers, films, revenue trends, and ETL pipeline health.

---

## ✨ Features

- **Interactive sidebar filters** on every analytics page
- **Real-time KPI cards** with conditional formatting
- **Beautiful Plotly visualizations** (Express + Graph Objects)
- **Responsive two-column layouts** and side-by-side charts
- **Raw data exploration** via expandable tables
- **CSV download buttons** with dynamic filenames (timestamped)
- **Cached data layer** (`@st.cache_data`) for fast performance
- **Manual refresh button** that clears all caches
- **Color-coded status indicators** and gauges
- **Search functionality** (Films page)

---

## 🛠️ Technologies Used

| Technology       | Purpose                              |
|------------------|--------------------------------------|
| **Streamlit**    | Web UI framework & dashboard         |
| **Plotly**       | Interactive charts (`px.bar`, `px.scatter`, `px.box`, `go.Figure`) |
| **Pandas**       | Data manipulation & filtering        |
| **SQLAlchemy**   | Database connection (via `db_utils`) |
| **PostgreSQL**   | Source of truth (dvdrental schema)   |
| **Caching**      | `@st.cache_data` (TTL = 5 minutes)   |

---

## 📁 Project Structure
```bash
sprint-05/day-31/
├── app.py                 # Main entry point + navigation
├── pages/
│   ├── overview.py        # Overview page
│   ├── customers.py       # Customer Analytics
│   └── films.py           # Film Analytics
└── db.py                  # Cached data loading layer

--- 

## 🚀 How to Run

```bash
cd sprint-05/day-31
streamlit run app.py
```

> **Note**: The app expects a running PostgreSQL instance with the `dvdrental` analytics tables (`analytics_customer_airflow`, `analytics_film_airflow`, `analytics_film_value_score`, `analytics_monthly_enriched`, `etl_audit_log`).

---

## 📊 Detailed Page Explanations

### 1. 📊 Overview Page (`overview.py`)

**Purpose**: High-level executive summary and pipeline monitoring.

**Key Sections**:
- **KPI Row 1** (4 cards):
  - Total Customers
  - Total Films
  - Total Revenue (`$x,xxx.xx`)
  - Platinum Customers
- **KPI Row 2** (3 cards):
  - Pipeline Runs
  - Successful Runs
  - Failed Runs
- **Monthly Revenue Trend**:
  - Combined **bar chart** (revenue) + **line chart** (MoM growth %) using `make_subplots` + `go.Bar` + `go.Scatter`
- **Recent Pipeline Runs** table with **color-coded status** (`success` = green, `failed` = red, `sla_miss` = orange)
- **Pipeline Success Rate Gauge** (`go.Indicator`) with threshold at 80%
- Raw data expanders + CSV downloads for monthly revenue and pipeline logs

---

### 2. 👥 Customer Analytics Page (`customers.py`)

**Purpose**: Deep dive into customer segmentation and spending behavior.

**Sidebar Filters**:
- **Segments** (multiselect): Bronze, Silver, Gold, Platinum (default = all)
- **Minimum Spend** (select_slider): $0 – $250

**Visualizations & Outputs**:
- **Metric**: Filtered Customers count
- **Bar Chart** (Plotly Express): Customers by Segment (filtered)
- **Scatter Plot**: Total Spend vs Total Rentals, colored by `value_segment`
- **Raw Data** expander showing full filtered dataframe
- **CSV Download** button (dynamic filename)

---

### 3. 🎬 Film Analytics Page (`films.py`)

**Purpose**: Analyze film inventory, value scoring, and rental performance.

**Sidebar Filters**:
- **Value Tier** (multiselect): Budget, Standard, Premium (default = all)
- **Rating** (multiselect): G, PG, PG-13, R, NC-17 (default = all)
- **Min Value Score** (slider): 0.0 – 100.0 (step = 1.0)
- **Search by Title** (text input) — applies before other filters

**Outputs**:
- **KPI Row** (3 metrics):
  - Filtered Films count
  - Avg Rental Rate (`$xx.xx`)
  - Avg Value Score (1 decimal place)
- **Two-column charts**:
  - **Left**: Bar chart — Film count by `value_tier`
  - **Right**: Box plot — Rental rate distribution by `rating`
- **Top 20 films** view (via raw data expander)
- Full filtered dataframe in expandable section
- CSV download button

---

## 🔧 Data Layer (`db.py`)

All data is loaded through cached functions:

- `load_customers()` → `analytics_customer_airflow`
- `load_films()` → Join between `analytics_film_value_score` and `analytics_film_airflow`
- `load_monthly_revenue()` → `analytics_monthly_enriched`
- `load_pipeline_status()` → `etl_audit_log`
- `get_summary_kpis()` → Aggregates data for overview KPIs

Caching TTL:
- Analytics tables → **5 minutes**
- Pipeline status → **1 minute**

---

## 🎯 Self-Explaining Dashboard

This README + the live app is **fully self-documenting**:

- Every page clearly states its purpose
- Sidebar filters are clearly labeled
- Charts have descriptive titles
- Raw data is always accessible via expanders
- All exports include timestamps

You can explore the entire analytics pipeline without needing any external documentation.

---

**Built with ❤️ for the Python Data Engineering Journey**  
**Last updated**: May 2026