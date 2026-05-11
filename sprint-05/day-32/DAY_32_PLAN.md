# 📅 DAY 32 — Sprint 05 | Streamlit Films Page + Revenue Chart
## Films Analytics, Monthly Revenue, Cache Invalidation, Refresh Button

---

## 🔁 RETROSPECTIVE — Day 31

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| app.py + sidebar | ✅ Pass | 3-page navigation working |
| db.py cached queries | ✅ Pass | @st.cache_data(ttl=300) |
| Overview KPIs | ✅ Pass | 599/1000/$61,312/21 — all correct |
| Pipeline gauge | ✅ Pass | Visible below fold |
| customers.py page | ✅ Pass | Filters + charts working |
| Failed runs = 15 | ⚠️ Fix | Apply on_failure final-failure guard |

### Pre-Start
```bash
# Fix on_failure callback first
# Open airflow/dags/airflow_callbacks.py in WSL2
# Add at top of on_failure():
#   ti = context["task_instance"]
#   if ti.try_number - 1 < ti.max_tries:
#       return   # not final failure — skip DB write

# Then create Day 32 branch
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-05/day-32-streamlit-films
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-07: Data Visualization & Reporting                        |
| Story           | ST-32: Streamlit Films Page + Revenue Chart + Cache          |
| Task ID         | TASK-032                                                     |
| Sprint          | Sprint 05 (Days 29–35)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | streamlit, films, revenue, cache, refresh, day-32            |
| Acceptance Criteria | Films page with value tier filter; monthly revenue chart on overview; manual refresh button clears cache; all 3 pages fully functional |

---

## 🎯 OBJECTIVES

1. Fix `on_failure_callback` final-failure guard
2. Build `pages/films.py` — film analytics with tier filter + value score chart
3. Add monthly revenue chart to `pages/overview.py`
4. Add manual refresh button that clears `@st.cache_data`
5. Add `components.py` — reusable chart + table components
6. Verify all 3 pages load correctly with live data
7. Push clean `[DAY-032][S05]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Callback fix + branch setup                        |
| B     | 40 min   | `pages/films.py` — full page                       |
| C     | 25 min   | Add revenue chart + refresh to overview             |
| D     | 20 min   | `components.py` — reusable widgets                 |
| E     | 20 min   | Run + verify + git push                            |

---

## 📝 EXERCISES

---

### EXERCISE 1 — pages/films.py (Block B)
**[Write yourself — follow the same pattern as customers.py]**

Create `sprint-05/day-31/pages/films.py`:

```python
"""
films.py — Film Analytics Page
================================
YOUR TASK: Build the films analytics page.

Requirements:
1. Page title: "🎬 Film Analytics"

2. Sidebar filters:
   - Value tier multiselect: ["Budget", "Standard", "Premium"]
     default = all three
   - Rating multiselect: ["G", "PG", "PG-13", "R", "NC-17"]
     default = all
   - Min value score slider: 0.0 to 100.0, step 1.0

3. KPI row (3 metrics):
   - Filtered Films count
   - Avg Rental Rate (mean of filtered, rounded to 2dp, prefix "$")
   - Avg Value Score (mean of filtered, rounded to 1dp)

4. Two charts side by side (use st.columns(2)):
   Left:  Bar chart — film count by value_tier (filtered)
          px.bar(x="value_tier", y="count", color="value_tier")

   Right: Box plot — rental_rate distribution by rating (filtered)
          px.box(x="rating", y="rental_rate", color="rating")
          title="Rental Rate by Rating"

5. Top 20 films table (filtered, sorted by value_score DESC):
   columns: [title, category, rating, value_tier, rental_rate, value_score]
   st.dataframe(..., use_container_width=True)

HINTS:
  from db import load_films
  df = load_films()

  # Sidebar filters (add BELOW app navigation in sidebar)
  with st.sidebar:
      st.markdown("---")
      tiers   = st.multiselect("Value Tier", ["Budget","Standard","Premium"],
                               default=["Budget","Standard","Premium"])
      ratings = st.multiselect("Rating", ["G","PG","PG-13","R","NC-17"],
                               default=["G","PG","PG-13","R","NC-17"])
      min_score = st.slider("Min Value Score", 0.0, 100.0, 0.0, step=1.0)

  filtered = df[
      (df["value_tier"].isin(tiers)) &
      (df["rating"].isin(ratings)) &
      (df["value_score"] >= min_score)
  ]

  # Charts in columns
  col1, col2 = st.columns(2)
  with col1:
      tier_counts = (filtered.groupby("value_tier")
                     .size().reset_index(name="count"))
      fig1 = px.bar(...)
      st.plotly_chart(fig1, use_container_width=True)

  with col2:
      fig2 = px.box(...)
      st.plotly_chart(fig2, use_container_width=True)

Self-check:
  - All 3 pages load without error
  - Filter to Premium only → ~336 films
  - Filter to G rating + all tiers → varies
  - Value score slider at 90 → small subset of highest-scoring films
"""
# YOUR CODE HERE
```

---

### EXERCISE 2 — Add Revenue Chart to Overview (Block C)
**[Full steps]**

Open `sprint-05/day-31/pages/overview.py` and add after the pipeline status table:

```python
# Add this import at top:
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from db import load_monthly_revenue

# Add after the gauge chart section:
st.markdown("---")
st.subheader("Monthly Revenue Trend")

monthly = load_monthly_revenue()
monthly["payment_date"] = pd.to_datetime(monthly["payment_date"])
monthly["month"] = monthly["payment_date"].dt.strftime("%b %Y")

fig_rev = make_subplots(specs=[[{"secondary_y": True}]])
fig_rev.add_trace(
    go.Bar(x=monthly["month"], y=monthly["total_revenue"],
           name="Revenue ($)", marker_color="steelblue", opacity=0.8),
    secondary_y=False,
)
valid = monthly["mom_growth_pct"].notna()
fig_rev.add_trace(
    go.Scatter(
        x=monthly.loc[valid, "month"],
        y=monthly.loc[valid, "mom_growth_pct"],
        name="MoM Growth (%)", mode="lines+markers",
        line=dict(color="tomato", width=2),
    ),
    secondary_y=True,
)
fig_rev.update_layout(
    title="Monthly Revenue + MoM Growth",
    template="plotly_dark",
    hovermode="x unified",
    height=350,
)
fig_rev.update_yaxes(title_text="Revenue ($)", secondary_y=False)
fig_rev.update_yaxes(title_text="MoM Growth (%)", secondary_y=True)
st.plotly_chart(fig_rev, use_container_width=True)
```

---

### EXERCISE 3 — Refresh Button + Cache Clear (Block C)
**[Full steps]**

Add a refresh button to `app.py` sidebar:

```python
# In app.py sidebar section, add:
with st.sidebar:
    st.title("🎬 DVD Rental")
    st.markdown("---")
    page = st.radio(...)
    st.markdown("---")

    # Manual refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()   # clears ALL cached data
        st.rerun()              # re-runs the entire script

    st.caption(f"Cache TTL: 5 min")
    st.caption("Python DE Journey — Day 32")
    st.caption("Data: dvdrental PostgreSQL")
```

---

### EXERCISE 4 — components.py: Reusable Widgets (Block D)
**[Write yourself — 3 simple functions]**

Create `sprint-05/day-31/components.py`:

```python
"""
components.py — Reusable Streamlit Components
==============================================
YOUR TASK: Implement 3 reusable UI helper functions.

HINTS:

def kpi_row(metrics: list[dict]) -> None:
    '''
    Display a row of metric cards.
    metrics = [{"label": "Customers", "value": 599, "delta": None}, ...]
    '''
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.metric(
            label=m["label"],
            value=m["value"],
            delta=m.get("delta"),
            delta_color=m.get("delta_color", "normal"),
        )

def status_table(df: pd.DataFrame, status_col: str = "status") -> None:
    '''
    Display a DataFrame with coloured status column.
    '''
    COLOR_MAP = {"success": "green", "failed": "red",
                 "sla_miss": "orange", "pending": "grey"}
    def colour_status(val):
        c = COLOR_MAP.get(str(val).lower(), "grey")
        return f"color: {c}; font-weight: bold"

    st.dataframe(
        df.style.applymap(colour_status, subset=[status_col]),
        use_container_width=True,
    )

def section_header(title: str, subtitle: str = "") -> None:
    '''Display a consistent section header with optional subtitle.'''
    st.subheader(title)
    if subtitle:
        st.caption(subtitle)
    st.markdown("---")
```

---

### EXERCISE 5 — Update app.py to Use Films Page

```python
# In app.py, replace the Films placeholder:
elif page == "🎬 Films":
    import pages.films as films
    films.render()
```

---

### EXERCISE 6 — Run + Verify

```bash
streamlit run sprint-05/day-31/app.py

# Check all 3 pages:
# Overview: KPIs + pipeline table + gauge + revenue chart
# Customers: segment filter + spend slider + 2 charts + table
# Films: tier filter + rating filter + score slider + 2 charts + top 20 table

# Test refresh button:
# Click "🔄 Refresh Data" → page reloads with fresh DB data
```

---

### EXERCISE 7 — Git Push

```bash
python scripts/daily_commit.py --day 32 --sprint 5 ^
    --message "Streamlit: films page with filters, revenue chart on overview, refresh button, components.py" ^
    --merge
```

---

## ✅ DAY 32 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `on_failure` final-failure guard applied in airflow_callbacks.py         | [ ]   |
| 2 | **`pages/films.py` written — tier+rating+score filters + 2 charts**      | [ ]   |
| 3 | Films page shows ~336 films when filtered to Premium                     | [ ]   |
| 4 | Box plot shows rental rate distribution by rating                        | [ ]   |
| 5 | Monthly revenue chart added to Overview page                             | [ ]   |
| 6 | Refresh button clears `st.cache_data` and reruns                         | [ ]   |
| 7 | **`components.py` written — kpi_row, status_table, section_header**      | [ ]   |
| 8 | All 3 pages render without error                                         | [ ]   |
| 9 | One clean `[DAY-032][S05]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 EXPECTED VALUES PER PAGE

```
Overview:
  Total Revenue: $61,312.04
  Monthly chart: 4 bars (Feb/Mar/Apr/May 2007)
  Peak month: April 2007 (~$28,559)

Customers (no filters):
  Count: 599
  Segments: Bronze~5, Silver~298, Gold~275, Platinum~21

Films (no filters):
  Count: 1000
  Premium: 336  Standard: 323  Budget: 341
  Box plot: NC-17 / R / PG-13 / PG / G distributions
```

---

## 🔜 PREVIEW: DAY 33

**Topic:** Dashboard polish + deployment prep  
**What you'll do:** Add a search box to films page, add pagination to large tables,
add `st.download_button` to export filtered data as CSV,
and write a `README` for the dashboard with screenshots and run instructions.

---

*Day 32 | Sprint 05 | EP-07 | TASK-032*
