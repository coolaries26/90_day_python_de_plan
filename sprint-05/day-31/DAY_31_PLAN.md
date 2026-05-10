# 📅 DAY 31 — Sprint 05 | Streamlit App Skeleton
## Install Streamlit, Live PostgreSQL, Sidebar Navigation, Plotly Charts

---

## 🔁 RETROSPECTIVE — Day 30

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| P1/P2 HTML + PNG | ✅ Pass | |
| P3 scatter 599 points | ✅ Pass | 129KB confirms all points rendered |
| P4 category treemap | ✅ Pass | |
| dashboard.html | ✅ Pass | 4-panel layout |
| PNG export (kaleido) | ✅ Pass | All 4 PNGs generated |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-05/day-31-streamlit
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-07: Data Visualization & Reporting                        |
| Story           | ST-31: Streamlit Dashboard — Skeleton + Live DB              |
| Task ID         | TASK-031                                                     |
| Sprint          | Sprint 05 (Days 29–35)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | streamlit, dashboard, postgresql, live-data, day-31          |
| Acceptance Criteria | Streamlit app runs locally; connects to PostgreSQL live; 3 pages with sidebar nav; Plotly charts render correctly |

---

## 📚 BACKGROUND

### What Streamlit Is

```python
# Streamlit: turn a Python script into a web app with zero HTML/CSS
# Every time a widget changes → script re-runs top to bottom
# No callbacks, no state management (mostly), no frontend code

import streamlit as st
import pandas as pd

st.title("My Dashboard")
df = pd.read_sql("SELECT * FROM film LIMIT 10", engine)
st.dataframe(df)           # interactive table
st.plotly_chart(fig)       # interactive Plotly chart
st.metric("Films", 1000)   # KPI card
```

### App Structure for Days 31–33

```
sprint-05/day-31/
├── app.py                  ← entry point (st.navigation)
├── pages/
│   ├── 01_overview.py      ← KPI cards + pipeline status
│   ├── 02_customers.py     ← customer analytics + charts
│   └── 03_films.py         ← film analytics + charts
├── db.py                   ← database connection + queries
└── components.py           ← reusable UI components
```

### Key Streamlit Commands

```python
# Layout
st.title("Title")
st.header("Header")
st.subheader("Subheader")
col1, col2, col3 = st.columns(3)   # side-by-side layout

# Data display
st.dataframe(df)           # interactive table
st.table(df)               # static table
st.json(dict)              # formatted JSON
st.metric("label", value, delta)   # KPI card with change indicator

# Charts
st.plotly_chart(fig, use_container_width=True)
st.pyplot(fig)             # matplotlib figure

# Widgets (cause re-run on change)
selected = st.selectbox("Choose", options)
value    = st.slider("Range", min, max)
text     = st.text_input("Search")
clicked  = st.button("Refresh")

# Sidebar
with st.sidebar:
    st.title("Navigation")
    page = st.radio("Go to", ["Overview", "Customers", "Films"])

# Caching — prevent re-querying DB on every widget interaction
@st.cache_data(ttl=300)   # cache for 5 minutes
def load_customers():
    return pd.read_sql("SELECT * FROM analytics_customer_airflow", engine)
```

---

## 🎯 OBJECTIVES

1. Install Streamlit
2. Build `db.py` — cached DB connection + query functions
3. Build `app.py` — entry point with sidebar navigation
4. Build `pages/01_overview.py` — KPI cards + pipeline status
5. Build `pages/02_customers.py` — customer charts (your Plotly P1 + P3)
6. Run the app and verify all pages load with live data
7. Push clean `[DAY-031][S05]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Install + branch setup                             |
| B     | 20 min   | `db.py` — cached queries                          |
| C     | 30 min   | `app.py` + `pages/01_overview.py`                  |
| D     | 30 min   | `pages/02_customers.py`                            |
| E     | 30 min   | Run + verify + git push                            |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install Streamlit (Block A)

```bash
.venv\Scripts\activate
pip install streamlit==1.29.0

# Verify
streamlit --version
# Expected: Streamlit, version 1.29.0

# Quick smoke test
streamlit hello   # opens browser with demo app — Ctrl+C to stop
```

---

### EXERCISE 2 — db.py: Cached Database Layer (Block B)
**[Fully provided — study @st.cache_data carefully]**

Create `sprint-05/day-31/db.py`:

```python
"""
db.py — Streamlit Database Layer
==================================
Cached query functions for the DVD Rental dashboard.
@st.cache_data(ttl=300) prevents re-querying on every widget interaction.

All functions return DataFrames or dicts — no DB objects leak out.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
import pandas as pd
import streamlit as st

# Add project paths
_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_root / "sprint-01" / "day-02"))

from db_utils import get_engine


def _engine():
    """Get or create SQLAlchemy engine. Reuses singleton from db_utils."""
    return get_engine()


@st.cache_data(ttl=300)   # cache 5 minutes — refresh every 5 min
def load_customers() -> pd.DataFrame:
    """Load full customer analytics table."""
    return pd.read_sql(
        "SELECT * FROM analytics_customer_airflow ORDER BY total_spend DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_films() -> pd.DataFrame:
    """Load film analytics table."""
    return pd.read_sql(
        "SELECT * FROM analytics_film_airflow ORDER BY value_score DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_monthly_revenue() -> pd.DataFrame:
    """Load monthly enriched revenue table."""
    return pd.read_sql(
        """SELECT payment_date, total_revenue, payment_count,
                  avg_payment, mom_growth_pct
           FROM analytics_monthly_enriched
           ORDER BY payment_date""",
        _engine()
    )


@st.cache_data(ttl=60)    # pipeline status refreshes every minute
def load_pipeline_status() -> pd.DataFrame:
    """Load recent pipeline run history from audit log."""
    return pd.read_sql(
        """SELECT pipeline_name, status, rows_loaded,
                  elapsed_s, run_at
           FROM etl_audit_log
           ORDER BY run_at DESC
           LIMIT 20""",
        _engine()
    )


@st.cache_data(ttl=300)
def get_summary_kpis() -> dict[str, Any]:
    """Return top-level KPI values for overview page."""
    customers = load_customers()
    films     = load_films()
    monthly   = load_monthly_revenue()
    pipeline  = load_pipeline_status()

    return {
        "total_customers":   len(customers),
        "active_customers":  int(customers["is_active"].sum())
            if "is_active" in customers.columns else 0,
        "total_films":       len(films),
        "total_revenue":     float(monthly["total_revenue"].sum()),
        "pipeline_runs":     len(pipeline),
        "successful_runs":   int((pipeline["status"] == "success").sum()),
        "failed_runs":       int((pipeline["status"] == "failed").sum()),
        "platinum_customers":int((customers["value_segment"] == "Platinum").sum())
            if "value_segment" in customers.columns else 0,
    }
```

---

### EXERCISE 3 — app.py: Entry Point + Sidebar (Block C)
**[Fully provided]**

Create `sprint-05/day-31/app.py`:

```python
"""
app.py — DVD Rental Analytics Dashboard
========================================
Main entry point for the Streamlit app.
Run: streamlit run sprint-05/day-31/app.py

Navigation: sidebar radio buttons → loads different pages
"""

import streamlit as st

# Page config — must be FIRST Streamlit command
st.set_page_config(
    page_title="DVD Rental Analytics",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar navigation
with st.sidebar:
    st.title("🎬 DVD Rental")
    st.markdown("---")
    page = st.radio(
        "Navigate to:",
        options=["📊 Overview", "👥 Customers", "🎬 Films"],
        index=0,
    )
    st.markdown("---")
    st.caption("Python DE Journey — Day 31")
    st.caption("Data: dvdrental PostgreSQL")

# Load selected page
if page == "📊 Overview":
    import pages.overview as overview
    overview.render()
elif page == "👥 Customers":
    import pages.customers as customers
    customers.render()
elif page == "🎬 Films":
    st.title("🎬 Films")
    st.info("Films page coming on Day 32")
```

---

### EXERCISE 4 — pages/overview.py (Block C)
**[Fully provided]**

```bash
mkdir sprint-05\day-31\pages
```

Create `sprint-05/day-31/pages/overview.py`:

```python
"""Overview page — KPI cards + pipeline status."""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_summary_kpis, load_pipeline_status


def render():
    st.title("📊 Overview")
    st.markdown("Real-time summary from the dvdrental analytics pipeline.")

    # ── KPI Cards ────────────────────────────────────────────────────────
    kpis = get_summary_kpis()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers",   kpis["total_customers"])
    col2.metric("Total Films",       kpis["total_films"])
    col3.metric("Total Revenue",     f"${kpis['total_revenue']:,.2f}")
    col4.metric("Platinum Customers",kpis["platinum_customers"])

    st.markdown("---")
    col5, col6, col7 = st.columns(3)
    col5.metric("Pipeline Runs",    kpis["pipeline_runs"])
    col6.metric("Successful Runs",  kpis["successful_runs"],
                delta=f"+{kpis['successful_runs']}", delta_color="normal")
    col7.metric("Failed Runs",      kpis["failed_runs"],
                delta=f"-{kpis['failed_runs']}" if kpis["failed_runs"] else "0",
                delta_color="inverse")

    # ── Pipeline Status Table ──────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Recent Pipeline Runs")

    pipeline_df = load_pipeline_status()

    # Colour status column
    def colour_status(val):
        colour = {"success": "green", "failed": "red", "sla_miss": "orange"}
        c = colour.get(val, "grey")
        return f"color: {c}; font-weight: bold"

    st.dataframe(
        pipeline_df.style.applymap(colour_status, subset=["status"]),
        use_container_width=True,
        height=350,
    )

    # ── Pipeline Success Rate Gauge ────────────────────────────────────────
    st.markdown("---")
    st.subheader("Pipeline Success Rate")

    total = kpis["pipeline_runs"]
    success_pct = (kpis["successful_runs"] / total * 100) if total > 0 else 0

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=success_pct,
        number={"suffix": "%"},
        delta={"reference": 80, "valueformat": ".1f"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar":  {"color": "green" if success_pct >= 80 else "red"},
            "steps": [
                {"range": [0, 50],   "color": "lightcoral"},
                {"range": [50, 80],  "color": "lightyellow"},
                {"range": [80, 100], "color": "lightgreen"},
            ],
            "threshold": {"line": {"color": "black", "width": 2},
                         "thickness": 0.75, "value": 80},
        },
        title={"text": "Success Rate vs 80% Target"},
    ))
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)
```

---

### EXERCISE 5 — pages/customers.py (Block D)
**[WRITE THIS YOURSELF — use P1 and P3 from Day 30]**

Create `sprint-05/day-31/pages/customers.py`:

```python
"""
customers.py — Customer Analytics Page
========================================
YOUR TASK: Build the customer analytics page.

Requirements:
1. Page title: "👥 Customer Analytics"
2. Filters in sidebar (use st.sidebar):
   - Segment multiselect (Bronze/Silver/Gold/Platinum)
   - Minimum spend slider (0 to 250)
3. Show filtered customer count as a metric
4. Show P1-style bar chart (segment distribution) using Plotly
5. Show P3-style scatter (spend vs rentals) using Plotly
6. Show filtered dataframe with columns:
   [customer_id, full_name (if available), value_segment, total_spend, total_rentals]

HINTS:
  from db import load_customers
  df = load_customers()

  # Sidebar filters
  with st.sidebar:
      segments = st.multiselect("Segments", ["Bronze","Silver","Gold","Platinum"],
                                default=["Bronze","Silver","Gold","Platinum"])
      min_spend = st.slider("Min Spend ($)", 0, 250, 0)

  # Apply filters
  filtered = df[
      (df["value_segment"].isin(segments)) &
      (df["total_spend"] >= min_spend)
  ]

  # Show metric
  st.metric("Filtered Customers", len(filtered))

  # Plotly segment bar
  import plotly.express as px
  fig_seg = px.bar(
      filtered.groupby("value_segment").size().reset_index(name="count"),
      x="value_segment", y="count", color="value_segment",
      title="Customers by Segment (filtered)"
  )
  st.plotly_chart(fig_seg, use_container_width=True)

  # Scatter
  fig_scatter = px.scatter(...)   # spend vs total_rentals

  # Dataframe
  st.dataframe(filtered[cols], use_container_width=True)
"""
# YOUR CODE HERE
```

---

### EXERCISE 6 — Run the App (Block E)

```bash
cd C:\90_day_python_de_plan
.venv\Scripts\activate

# Run from project root
streamlit run sprint-05/day-31/app.py

# Opens: http://localhost:8501
# Verify:
#   Overview page: KPI cards show real values
#   Customers page: charts load from PostgreSQL
#   Films page: shows "coming Day 32" message
```

---

### EXERCISE 7 — Git Push

```bash
python scripts/daily_commit.py --day 31 --sprint 5 ^
    --message "Streamlit: app skeleton, db.py cached queries, overview KPIs, customer page with filters" ^
    --merge
```

---

## ✅ DAY 31 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | Streamlit 1.29.0 installed                                               | [ ]   |
| 2 | `db.py` created — 5 cached query functions                               | [ ]   |
| 3 | `app.py` runs — sidebar navigation working                               | [ ]   |
| 4 | Overview page: 7 KPI cards showing real values                           | [ ]   |
| 5 | Overview page: pipeline status table with coloured status column         | [ ]   |
| 6 | Overview page: success rate gauge chart                                  | [ ]   |
| 7 | **`pages/customers.py` written — segment filter + scatter + bar**        | [ ]   |
| 8 | Customer filters (segment multiselect + spend slider) work correctly     | [ ]   |
| 9 | `streamlit run app.py` runs without errors                               | [ ]   |
|10 | One clean `[DAY-031][S05]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK

```
http://localhost:8501

Overview page should show:
  Total Customers: 599
  Total Films:     1000
  Total Revenue:   ~$61,312
  Platinum:        ~21

Customers page should show:
  With no filters: 599 customers
  Filter to Platinum only: ~21 customers
  Charts update dynamically when filters change
```

---

## 🔜 PREVIEW: DAY 32

**Topic:** Streamlit films page + monthly revenue chart + data refresh  
**What you'll do:** Build the Films page with value tier filters,
add the monthly revenue chart to Overview, implement `st.cache_data` invalidation,
and add a manual refresh button.

---

*Day 31 | Sprint 05 | EP-07 | TASK-031*
