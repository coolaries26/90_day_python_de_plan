# 📅 DAY 33 — Sprint 05 | Dashboard Polish + Download + README
## Search, Pagination, CSV Download, Deployment README

---

## 🔁 RETROSPECTIVE — Day 32

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| films.py — 3 filters | ✅ Pass | Value tier + rating + score slider |
| Box plot rental rate | ✅ Pass | |
| Revenue chart on overview | ✅ Pass | |
| Refresh button | ✅ Pass | cache_data.clear() + rerun |
| components.py | ✅ Pass | |
| load_films() SQL fix | ✅ Pass | JOIN to analytics_film_value_score |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-05/day-33-dashboard-polish
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-07: Data Visualization & Reporting                        |
| Story           | ST-33: Dashboard Polish + Download + README                  |
| Task ID         | TASK-033                                                     |
| Sprint          | Sprint 05 (Days 29–35)                                       |
| Story Points    | 2                                                            |
| Priority        | HIGH                                                         |
| Labels          | streamlit, polish, download, readme, deployment, day-33      |
| Acceptance Criteria | CSV download on all pages; search on films; README with run instructions; app starts from single command |

---

## 🎯 OBJECTIVES

1. Add `st.download_button` to export filtered data as CSV on all pages
2. Add search box to films page (title search)
3. Add `st.expander` for raw data toggle on all pages
4. Write `README.md` for the dashboard with run instructions + screenshots
5. Create `run_dashboard.bat` — one-click Windows launcher
6. Fix `db.py` — consolidate `load_films()` join as permanent
7. Push clean `[DAY-033][S05]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Branch setup                                       |
| B     | 30 min   | CSV download + search + expander on all pages      |
| C     | 30 min   | `README.md` for dashboard                          |
| D     | 20 min   | `run_dashboard.bat` + final testing                |
| E     | 30 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — CSV Download Button (Block B)
**[Full steps — add to all 3 pages]**

**Pattern — add this at the bottom of each page's `render()` function:**

```python
# ── CSV Export ────────────────────────────────────────────────────────────
st.markdown("---")
col_dl, col_info = st.columns([1, 3])
with col_dl:
    csv_bytes = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download CSV",
        data=csv_bytes,
        file_name=f"customers_filtered_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True,
    )
with col_info:
    st.caption(f"Downloading {len(filtered):,} rows × {len(filtered.columns)} columns")
```

**Add to each page:**
- `pages/customers.py` → downloads filtered customers
- `pages/films.py` → downloads filtered films (use appropriate filename)
- `pages/overview.py` → downloads pipeline status log

---

### EXERCISE 2 — Search Box on Films Page (Block B)
**[Full steps — add BEFORE the tier/rating filters]**

Open `pages/films.py` and add a title search above the other filters:

```python
# In sidebar, BEFORE the tier multiselect:
with st.sidebar:
    st.markdown("---")
    search = st.text_input("🔍 Search by Title", placeholder="e.g. academy")

    tiers   = st.multiselect(...)
    ratings = st.multiselect(...)
    min_score = st.slider(...)

# Apply search filter FIRST, then other filters:
filtered = df.copy()
if search:
    filtered = filtered[
        filtered["title"].str.contains(search, case=False, na=False)
    ]
filtered = filtered[
    (filtered["value_tier"].isin(tiers)) &
    (filtered["rating"].isin(ratings)) &
    (filtered["value_score"] >= min_score)
]

# Show search result count
if search:
    st.caption(f"🔍 '{search}' matched {len(filtered)} films")
```

---

### EXERCISE 3 — Raw Data Expander (Block B)
**[Write yourself — one pattern, apply to all 3 pages]**

```python
# Add at bottom of each page, ABOVE the download button:

with st.expander("📋 View Raw Data", expanded=False):
    st.caption(f"{len(filtered):,} rows × {len(filtered.columns)} columns")
    st.dataframe(filtered, use_container_width=True, height=300)
```

The `expanded=False` means it's collapsed by default — doesn't clutter the page but is available when needed.

---

### EXERCISE 4 — Dashboard README.md (Block C)
**[Write yourself — use the template below as a guide]**

Create `sprint-05/day-31/README.md`:

```markdown
# 🎬 DVD Rental Analytics Dashboard

Interactive analytics dashboard built with Streamlit, Plotly, and PostgreSQL.

## Screenshots

| Overview | Customers | Films |
|----------|-----------|-------|
| ![Overview](../../sprint-05/day-31/output/overview_screenshot.png) | ... | ... |

## Quick Start

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

### Pages
| Page | Description |
|------|-------------|
| 📊 Overview | KPI cards, pipeline status, revenue trend |
| 👥 Customers | Segment analysis, spend distribution, filters |
| 🎬 Films | Value tier analysis, rating distribution, search |

### Data Sources (PostgreSQL dvdrental)
| Table | Description |
|-------|-------------|
| analytics_customer_airflow | Customer segments + spend metrics |
| analytics_film_airflow | Film value tiers |
| analytics_film_value_score | Film value scores + rental counts |
| analytics_monthly_enriched | Monthly revenue + MoM growth |
| etl_audit_log | Pipeline run history |

### Features
- Live PostgreSQL connection
- 5-minute data cache (configurable)
- Manual refresh button
- Interactive Plotly charts
- CSV download on all pages
- Title search on films page

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

**Add your own actual content** — edit the template with real descriptions and fix the screenshot paths.

---

### EXERCISE 5 — run_dashboard.bat (Block D)
**[Full steps]**

Create `sprint-05/day-31/run_dashboard.bat`:

```batch
@echo off
echo ============================================
echo  DVD Rental Analytics Dashboard
echo  Python DE Journey
echo ============================================
echo.

cd /d C:\90_day_python_de_plan

echo Activating virtual environment...
call .venv\Scripts\activate

echo Starting Streamlit dashboard...
echo Open: http://localhost:8501
echo Press Ctrl+C to stop.
echo.

streamlit run sprint-05/day-31/app.py --server.port 8501 --browser.gatherUsageStats false

pause
```

**Test:**
```bash
# Double-click run_dashboard.bat in Windows Explorer
# OR from command prompt:
sprint-05\day-31\run_dashboard.bat
```

---

### EXERCISE 6 — Final Test All Pages

```bash
streamlit run sprint-05/day-31/app.py

# Verify on each page:
# Overview:
#   - ⬇️ Download CSV downloads pipeline_status.csv
#   - Revenue chart shows 4 months
#   - Refresh button works

# Customers:
#   - Filter to Platinum → 21 customers, download shows 21 rows
#   - Set min spend to $200 → small subset
#   - Raw data expander shows full DataFrame

# Films:
#   - Search "academy" → "Academy Dinosaur" + others
#   - Filter to Premium + G rating → small subset
#   - Download CSV = filtered result only
```

---

### EXERCISE 7 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 33 --sprint 5 ^
    --message "Dashboard polish: CSV download, title search, raw data expander, README, run_dashboard.bat" ^
    --merge
```

---

## ✅ DAY 33 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | CSV download on Overview page                                            | [ ]   |
| 2 | CSV download on Customers page (respects filters)                        | [ ]   |
| 3 | CSV download on Films page (respects filters)                            | [ ]   |
| 4 | Title search on Films page — case-insensitive                            | [ ]   |
| 5 | Raw data expander on all 3 pages (collapsed by default)                  | [ ]   |
| 6 | `README.md` created with run instructions + architecture                 | [ ]   |
| 7 | `run_dashboard.bat` works from double-click                              | [ ]   |
| 8 | `load_films()` JOIN in db.py is permanent (not a workaround)             | [ ]   |
| 9 | All 3 pages load without errors                                          | [ ]   |
|10 | One clean `[DAY-033][S05]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK

```
Search "academy" on Films page:
  → Should show: Academy Dinosaur, Academy Boondock (and others with 'academy')

Filter Films to Premium + G rating:
  → Should show a small subset (G-rated Premium films)
  → Download CSV should contain only those rows

Download on Customers filtered to Platinum ($200+ spend):
  → CSV should have ~10-15 rows, not 599
```

---

## 🔜 PREVIEW: DAY 34

**Topic:** Airflow DAG that runs the chart generation automatically  
**What you'll do:** Create `dag_chart_generator.py` — an Airflow DAG that runs
`charts.py` and `plotly_charts.py` after the ETL pipelines complete (using Dataset triggers).
Charts are automatically refreshed every time new data is loaded.

---

*Day 33 | Sprint 05 | EP-07 | TASK-033*
