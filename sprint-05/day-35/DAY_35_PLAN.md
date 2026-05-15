# 📅 DAY 35 — Sprint 05 Test + Sprint Close
## Visualization + Streamlit + Chart Pipeline Assessment

---

## 🔁 RETROSPECTIVE — Day 34

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| generate_static_charts | ✅ Pass | 5/5 PNGs |
| generate_interactive_charts | ⚠️ Fix | p3 missing — statsmodels or trendline |
| manifest.json | ✅ Pass | Structure correct |
| CHART_DATASET outlet | ✅ Pass | |
| Dataset trigger | ✅ Pass | Needed manual unpause (expected) |
| WSL2 lib install | ✅ Fixed | matplotlib/plotly/seaborn added |

### Fix Before Starting
```bash
# WSL2 — fix p3 missing chart
# Option A: install statsmodels
source ~/airflow-venv/bin/activate
pip install statsmodels

# Option B: remove trendline argument from p3 (simpler)
# Edit sprint-05/day-30/plotly_charts.py line with trendline="ols"
# Remove: trendline="ols"

# Retrigger to verify total_charts = 9
airflow dags trigger dag_chart_generator
sleep 60
cat /c/90_day_python_de_plan/airflow/output/charts/manifest.json | grep total_charts

# Create Day 35 branch
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-05/day-35-sprint-test
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-07: Data Visualization & Reporting                        |
| Story           | ST-35: Sprint 05 Test + Assessment                           |
| Task ID         | TASK-035                                                     |
| Sprint          | Sprint 05 (Days 29–35)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | sprint-test, assessment, visualization, streamlit, day-35    |
| Acceptance Criteria | All 4 test tasks pass; sprint-05-complete tag created; Sprint 06 (ML) unlocked |

---

## 📚 SPRINT 05 TEST — RULES

```
1. No looking at previous day plans during test tasks
2. 90-minute time box total
3. Document blockers and move on after time box
4. Honest self-scoring
```

---

## 🎯 SPRINT 05 TEST — 4 TASKS

**Time box: 90 minutes total**

---

### TASK T1 — New Chart from Scratch (25 min)

**Brief:** Without referencing `charts.py`, write a new chart function
`chart_rental_timeline()` in a new file `sprint-05/day-35/test_charts.py`.

**Requirements:**
- Source: query the `rental` table directly (not analytics tables)
- Chart type: line chart showing rental count per week
- X axis: week start date (use `DATE_TRUNC('week', rental_date)`)
- Y axis: rental count
- Add a horizontal line for the average weekly rentals
- Save as PNG to `sprint-05/day-35/output/rental_timeline.png`
- Use `matplotlib.use("Agg")` — no display

**SQL hint:**
```sql
SELECT DATE_TRUNC('week', rental_date)::date AS week_start,
       COUNT(*) AS rental_count
FROM rental
GROUP BY DATE_TRUNC('week', rental_date)
ORDER BY week_start
```

**Pass criteria:**
```bash
python sprint-05/day-35/test_charts.py
ls sprint-05/day-35/output/rental_timeline.png
# File exists, size > 20KB
```

---

### TASK T2 — Streamlit New Page (25 min)

**Brief:** Add a 4th page to the dashboard: `pages/rentals.py`

**Requirements:**
1. Page title: "📅 Rental Analytics"
2. Load rental data via a new `load_rental_stats()` function in `db.py`:
   ```sql
   SELECT DATE_TRUNC('week', rental_date)::date AS week_start,
          COUNT(*) AS rental_count,
          COUNT(DISTINCT customer_id) AS unique_customers,
          COUNT(CASE WHEN return_date IS NULL THEN 1 END) AS still_open
   FROM rental
   GROUP BY DATE_TRUNC('week', rental_date)
   ORDER BY week_start
   ```
3. Show 3 KPI metrics: Total Rentals, Open Rentals, Unique Customers
4. Show weekly rental trend as a Plotly line chart
5. Add page to `app.py` sidebar navigation

**Pass criteria:**
```
http://localhost:8501
Navigate to "📅 Rental Analytics"
Chart shows weekly rental trend
3 KPI metrics visible with correct values:
  Total Rentals: 16,044
  Open Rentals: 183
  Unique Customers: 599
```

---

### TASK T3 — Chart Pipeline (15 min)

**Brief:** Add `chart_rental_timeline()` to `dag_chart_generator.py`.

**Requirements:**
- Import from `test_charts.py` (or move function to `charts.py`)
- Add as a 6th task in `generate_static_charts`
- Retrigger DAG manually
- Verify manifest shows `total_charts: 10` (9 original + 1 new)

**Pass criteria:**
```bash
airflow dags trigger dag_chart_generator
sleep 60
cat airflow/output/charts/manifest.json | grep total_charts
# "total_charts": 10
```

---

### TASK T4 — Self-Written: Plotly + Streamlit Integration (25 min)

**Brief:** On the new Rentals page, add an interactive Plotly chart
that shows the weekly rental trend with:
- Hoverable data points showing week_start + rental_count + unique_customers
- A filled area under the line (`fill="tozeroy"`)
- Annotations marking the peak week

```python
# HINT — Plotly filled area:
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["week_start"],
    y=df["rental_count"],
    fill="tozeroy",
    mode="lines+markers",
    hovertemplate="Week: %{x}<br>Rentals: %{y}<br>Customers: %{customdata}<extra></extra>",
    customdata=df["unique_customers"],
    name="Weekly Rentals",
))

# Find peak week and annotate:
peak_idx = df["rental_count"].idxmax()
fig.add_annotation(
    x=df.loc[peak_idx, "week_start"],
    y=df.loc[peak_idx, "rental_count"],
    text=f"Peak: {df.loc[peak_idx, 'rental_count']:,}",
    showarrow=True, arrowhead=2,
)
```

**Pass criteria:**
- Chart renders in Streamlit with hover + fill
- Peak week annotation visible
- `streamlit run app.py` shows all 4 pages without error

---

## 📊 SPRINT 05 SCORING RUBRIC

| Task | Max | Your Score | Notes |
|------|-----|------------|-------|
| T1: rental_timeline.png > 20KB | 20 | | |
| T2: Rentals page — 3 KPIs + chart | 25 | | |
| T3: manifest shows total_charts: 10 | 15 | | |
| T4: Plotly filled area + annotation | 20 | | |
| p3 fixed (total_charts was 8→9) | 5 | | |
| Code quality: no print(), proper logging | 5 | | |
| Git: one clean [DAY-035][S05] commit | 10 | | |
| **TOTAL** | **100** | | |

**Thresholds:**
```
≥85  → Sprint 06 (ML Foundations) starts Day 36
70–84 → Sprint 06 with one remediation task
<70  → Two remediation days
```

---

## 📤 SPRINT CLOSE

```bash
cd C:\90_day_python_de_plan

# Commit Day 35
python scripts/daily_commit.py --day 35 --sprint 5 ^
    --message "Sprint 05 test: rental chart, rentals page, chart DAG +1, filled area Plotly" ^
    --merge

# Close Sprint 05
python scripts/daily_commit.py --day 35 --sprint 5 ^
    --message "Sprint 05 complete" ^
    --to-main

# Verify all sprint tags
git tag
# sprint-01-complete through sprint-05-complete
```

---

## ✅ DAY 35 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | p3_spend_vs_rentals.html fixed — total_charts = 9                        | [ ]   |
| 2 | `rental_timeline.png` generated > 20KB                                   | [ ]   |
| 3 | Rentals page loads in Streamlit with 3 KPIs                              | [ ]   |
| 4 | Weekly rental trend chart in Streamlit                                   | [ ]   |
| 5 | Rentals page added to app.py sidebar                                     | [ ]   |
| 6 | Chart DAG manifest shows total_charts: 10                                | [ ]   |
| 7 | Plotly filled area + peak annotation on Rentals page                     | [ ]   |
| 8 | All 4 pages load without error in Streamlit                              | [ ]   |
| 9 | `sprint-05-complete` tag created                                         | [ ]   |
|10 | All 5 sprint tags visible in `git tag`                                   | [ ]   |

---

## 🔍 SELF-CHECK

```bash
# All pages working:
streamlit run sprint-05/day-31/app.py &
sleep 5
curl -s http://localhost:8501 | grep -c "Streamlit"
# Should return 1

# Chart count in manifest:
cat airflow/output/charts/manifest.json | python -c "
import json, sys
m = json.load(sys.stdin)
print(f'Static:      {len(m[\"static_charts\"])}')
print(f'Interactive: {len(m[\"interactive_charts\"])}')
print(f'Total:       {m[\"total_charts\"]}')
"
# Static:      6  (original 5 + rental_timeline)
# Interactive: 4  (original 3 fixed + p3)
# Total:       10
```

---

## 🔜 PREVIEW: SPRINT 06 — Day 36

**Topic:** ML Foundations — NumPy + scikit-learn  
**Stack:** `numpy`, `scikit-learn`, `joblib`  
**What you'll do:**
- NumPy array operations on analytics data
- Feature engineering from DVD Rental tables
- Train first ML model: customer churn prediction
- Evaluate with cross-validation
- Save model with joblib

**Data:** `analytics_customer_airflow` → customer churn features

---

*Day 35 | Sprint 05 | EP-07 | TASK-035*
