# 📅 DAY 47 — Sprint 07 | Capstone Streamlit Dashboard
## 5-Page E-Commerce Analytics Dashboard — Portfolio Piece

---

## 🔁 RETROSPECTIVE — Day 46

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| All 4 tasks green | ✅ Pass | Total pipeline: ~7 min |
| ETL 1m32s | ✅ Pass | Realistic for 96k rows |
| Models <5MB each | ✅ Pass | 99% reduction from 407MB |
| All 4 tables updated | ✅ Pass | |
| Parallel task pattern | ✅ Pass | churn + delay run in parallel |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-07/day-47-dashboard
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-10: Capstone & Job Readiness |
| Story           | ST-47: Capstone Dashboard |
| Task ID         | TASK-047 |
| Sprint          | Sprint 07 (Days 43–48) |
| Story Points    | 3 |
| Priority        | CRITICAL |
| Labels          | streamlit, dashboard, capstone, portfolio, day-47 |
| Acceptance Criteria | 5-page dashboard live; all pages load with live data; ML predictions visible; exportable as demo |

---

## 🎯 OBJECTIVES

Build a 5-page Streamlit dashboard on the e-commerce analytics data.

```
capstone/dashboard/
├── app.py              ← entry point
├── db.py               ← cached queries
├── pages/
│   ├── overview.py     ← KPIs + revenue trend
│   ├── orders.py       ← delivery analysis
│   ├── customers.py    ← LTV + churn risk
│   ├── sellers.py      ← seller performance
│   └── ml_insights.py ← predictions + model info
└── run_dashboard.bat   ← launcher
```

---

## ⏱️ TIME BUDGET (2.5 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Scaffold + db.py |
| B | 25 min | app.py + overview.py |
| C | 25 min | orders.py |
| D | 30 min | customers.py |
| E | 25 min | sellers.py + ml_insights.py |
| F | 15 min | run_dashboard.bat + smoke test |
| G | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — capstone/dashboard/db.py (Block A)
**[Fully provided]**

Create `capstone/dashboard/db.py`:

```python
"""capstone/dashboard/db.py — Cached queries for e-commerce dashboard."""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_ecommerce_engine


def _engine():
    return get_ecommerce_engine()


@st.cache_data(ttl=300)
def load_customer_ltv() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM analytics.customer_ltv", _engine())


@st.cache_data(ttl=300)
def load_order_metrics() -> pd.DataFrame:
    return pd.read_sql("""
        SELECT * FROM analytics.order_metrics
        WHERE delivery_days_actual IS NOT NULL
    """, _engine())


@st.cache_data(ttl=300)
def load_seller_performance() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM analytics.seller_performance ORDER BY total_revenue DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_product_analytics() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM analytics.product_analytics ORDER BY total_revenue DESC",
        _engine()
    )


@st.cache_data(ttl=300)
def load_monthly_revenue() -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM analytics.monthly_revenue ORDER BY order_month",
        _engine()
    )


@st.cache_data(ttl=300)
def load_churn_predictions() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM ml.churn_predictions", _engine())


@st.cache_data(ttl=300)
def load_delay_predictions() -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM ml.delay_predictions", _engine())


@st.cache_data(ttl=60)
def get_kpis() -> dict:
    customers = load_customer_ltv()
    orders    = load_order_metrics()
    monthly   = load_monthly_revenue()
    churn     = load_churn_predictions()

    return {
        "total_customers":   len(customers),
        "total_orders":      len(orders),
        "total_revenue":     float(monthly["total_revenue"].sum()),
        "avg_review_score":  float(orders["review_score"].mean()),
        "late_delivery_rate":float(orders["is_late"].mean()),
        "churn_rate":        float(churn["predicted_churn"].mean())
            if "predicted_churn" in churn.columns else 0.0,
        "avg_ltv":           float(customers["total_spent"].mean()),
        "repeat_customers":  int((customers["is_churned"] == 0).sum()),
    }
```

---

### EXERCISE 2 — app.py + overview.py (Block B)
**[app.py fully provided. overview.py — write yourself]**

Create `capstone/dashboard/app.py`:

```python
"""capstone/dashboard/app.py — E-Commerce Analytics Dashboard."""
import streamlit as st

st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    st.title("🛒 E-Commerce Analytics")
    st.caption("Olist Brazilian E-Commerce")
    st.markdown("---")
    page = st.radio("Navigate to:", options=[
        "📊 Overview",
        "📦 Orders",
        "👥 Customers",
        "🏪 Sellers",
        "🤖 ML Insights",
    ])
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Python DE Journey — Day 47")
    st.caption("100k orders | 2016-2018")

if page == "📊 Overview":
    import pages.overview as p; p.render()
elif page == "📦 Orders":
    import pages.orders as p; p.render()
elif page == "👥 Customers":
    import pages.customers as p; p.render()
elif page == "🏪 Sellers":
    import pages.sellers as p; p.render()
elif page == "🤖 ML Insights":
    import pages.ml_insights as p; p.render()
```

Create `capstone/dashboard/pages/__init__.py` (empty file).

**Create `capstone/dashboard/pages/overview.py` — WRITE THIS YOURSELF:**
```python
"""
overview.py — Overview page
YOUR TASK: Build the overview page.

Requirements:
1. Title: "📊 Business Overview"
2. 4 KPI cards (use st.columns(4)):
   - Total Customers
   - Total Orders
   - Total Revenue (format as $X,XXX,XXX.XX)
   - Avg Review Score (format as X.XX ⭐)
3. 3 more KPI cards:
   - Late Delivery Rate (as %)
   - Repeat Customers
   - Avg Customer LTV ($)
4. Monthly Revenue chart (Plotly bar + line for order count)
   Use load_monthly_revenue() from db.py
   x="order_month", y="total_revenue"
5. Top 5 product categories by revenue (horizontal bar chart)
   Use load_product_analytics() from db.py

HINTS:
  from db import get_kpis, load_monthly_revenue, load_product_analytics
  import plotly.express as px
  import plotly.graph_objects as go

  kpis = get_kpis()
  col1, col2, col3, col4 = st.columns(4)
  col1.metric("Total Customers", f"{kpis['total_customers']:,}")
  col2.metric("Total Orders", f"{kpis['total_orders']:,}")
  col3.metric("Total Revenue", f"${kpis['total_revenue']:,.2f}")
  col4.metric("Avg Review", f"{kpis['avg_review_score']:.2f} ⭐")
"""
# YOUR CODE HERE
```

---

### EXERCISE 3 — orders.py (Block C)
**[Write yourself]**

Create `capstone/dashboard/pages/orders.py`:
```python
"""
orders.py — Orders + Delivery Analysis page
YOUR TASK: Build the orders page.

Requirements:
1. Title: "📦 Order & Delivery Analytics"
2. Sidebar filter: Delivery Status (All / On Time / Late)
3. 3 KPI metrics:
   - Total Orders (filtered)
   - Late Delivery Rate (%)
   - Avg Delivery Days
4. Key insight box (st.info or st.warning):
   "⚡ Late orders receive avg X.XX ⭐ vs Y.YY ⭐ for on-time orders"
   (Use the 4.29 vs 2.57 finding from Day 44)
5. Delivery days histogram (px.histogram, x="delivery_days_actual", nbins=30)
6. Review score by is_late (px.box, x="is_late", y="review_score")
7. CSV download button

HINTS:
  from db import load_order_metrics
  df = load_order_metrics()

  # Sidebar filter
  with st.sidebar:
      st.markdown("---")
      status = st.selectbox("Delivery Status", ["All","On Time","Late"])

  filtered = df.copy()
  if status == "On Time":  filtered = df[df["is_late"] == 0]
  if status == "Late":     filtered = df[df["is_late"] == 1]
"""
# YOUR CODE HERE
```

---

### EXERCISE 4 — customers.py + sellers.py (Block D + E)
**[Write yourself — follow orders.py pattern]**

Create `capstone/dashboard/pages/customers.py`:
```python
"""
customers.py — Customer LTV + Churn Risk page
Requirements:
1. Title: "👥 Customer Analytics"
2. Sidebar filter: Value Segment (All/Bronze/Silver/Gold/Platinum)
3. KPIs: Total Customers, Repeat Rate, Avg LTV, Avg Review
4. LTV histogram (px.histogram, x="total_spent", nbins=40)
5. Scatter: total_spent vs total_orders, colored by value_segment
6. Churn risk table: customers with is_churned=1 and total_spent > 200
   (these are high-value customers who churned — priority retention targets)
7. Download CSV
"""
# YOUR CODE HERE
```

Create `capstone/dashboard/pages/sellers.py`:
```python
"""
sellers.py — Seller Performance page
Requirements:
1. Title: "🏪 Seller Performance"
2. Sidebar filter: State (multiselect from seller_state column)
3. KPIs: Total Sellers, Avg Revenue, Avg Rating, Avg On-Time Rate
4. Scatter: total_revenue vs avg_review_score, colored by on_time_delivery_rate
5. Top 10 sellers table (by total_revenue)
6. Bar chart: avg on_time_delivery_rate by seller_state
"""
# YOUR CODE HERE
```

---

### EXERCISE 5 — ml_insights.py (Block E)
**[Fully provided — connects ML predictions to dashboard]**

Create `capstone/dashboard/pages/ml_insights.py`:

```python
"""ml_insights.py — ML Predictions + Model Insights."""
import sys
import json
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dashboard.db import load_churn_predictions, load_delay_predictions


MODEL_DIR = Path(__file__).resolve().parent.parent.parent / "ml" / "models"


def render():
    st.title("🤖 ML Model Insights")

    tab1, tab2, tab3 = st.tabs(["Churn Risk", "Delivery Delay", "Model Info"])

    # ── Tab 1: Churn Risk ─────────────────────────────────────────────────
    with tab1:
        st.subheader("Customer Churn Predictions")
        churn_df = load_churn_predictions()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Customers",    f"{len(churn_df):,}")
        col2.metric("Predicted Churned",  f"{churn_df['predicted_churn'].sum():,}")
        col3.metric("Churn Rate",
                    f"{churn_df['predicted_churn'].mean():.1%}")

        # Churn probability distribution
        fig = px.histogram(
            churn_df, x="churn_probability", nbins=30,
            title="Churn Probability Distribution",
            color_discrete_sequence=["tomato"],
            template="plotly_dark",
        )
        fig.add_vline(x=0.5, line_dash="dash", line_color="white",
                      annotation_text="Decision threshold (0.5)")
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Delivery Delay ─────────────────────────────────────────────
    with tab2:
        st.subheader("Delivery Delay Predictions")
        delay_df = load_delay_predictions()

        col1, col2, col3 = st.columns(3)
        late_col = [c for c in delay_df.columns if "late" in c.lower() and "predict" in c.lower()]
        if late_col:
            col1.metric("Total Orders",        f"{len(delay_df):,}")
            col2.metric("Predicted Late",      f"{delay_df[late_col[0]].sum():,}")
            col3.metric("Predicted Late Rate", f"{delay_df[late_col[0]].mean():.1%}")

        # Late probability distribution
        prob_col = [c for c in delay_df.columns if "prob" in c.lower()]
        if prob_col:
            fig2 = px.histogram(
                delay_df, x=prob_col[0], nbins=30,
                title="Late Delivery Probability Distribution",
                color_discrete_sequence=["orange"],
                template="plotly_dark",
            )
            st.plotly_chart(fig2, use_container_width=True)

    # ── Tab 3: Model Info ─────────────────────────────────────────────────
    with tab3:
        st.subheader("Model Metadata")

        for model_name in ["churn_pipeline", "delay_pipeline"]:
            meta_path = MODEL_DIR / f"{model_name}_metadata.json"
            if meta_path.exists():
                with open(meta_path) as f:
                    meta = json.load(f)
                with st.expander(f"📋 {model_name}", expanded=True):
                    st.json(meta)

        st.markdown("---")
        st.info(
            "**Model Limitations:**\n\n"
            "- Churn model CV F1 ≈ 0.22 — limited by feature availability "
            "(purchase history excluded to prevent leakage)\n"
            "- Delay model optimised for recall over precision — "
            "better to flag potential delays early\n"
            "- Both models trained on 2016-2018 data — "
            "seasonal patterns may have shifted"
        )
```

---

### EXERCISE 6 — run_dashboard.bat + Smoke Test (Block F)

Create `capstone/dashboard/run_dashboard.bat`:
```batch
@echo off
echo ============================================
echo  E-Commerce Analytics Dashboard
echo  Python DE Journey — Capstone
echo ============================================
cd /d C:\90_day_python_de_plan
call .venv\Scripts\activate
streamlit run capstone/dashboard/app.py --server.port 8502 ^
    --browser.gatherUsageStats false
pause
```

Use port 8502 (8501 is the dvdrental dashboard).

```bash
# Smoke test
streamlit run capstone/dashboard/app.py --server.port 8502 --server.headless true &
sleep 5
curl -s http://localhost:8502 | grep -c "Streamlit"
pkill -f "capstone/dashboard" 2>/dev/null
# Should return 1
```

---

### EXERCISE 7 — Git Push

```bash
python scripts/daily_commit.py --day 47 --sprint 7 ^
    --message "Capstone dashboard: 5 pages, live PostgreSQL, churn+delay predictions, portfolio-ready" ^
    --merge
```

---

## ✅ DAY 47 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `db.py` created — 8 cached query functions | [ ] |
| 2 | `app.py` runs — sidebar navigation working | [ ] |
| 3 | **`overview.py` written — KPIs + revenue chart + top categories** | [ ] |
| 4 | **`orders.py` written — delivery analysis + late vs on-time insight** | [ ] |
| 5 | **`customers.py` written — LTV histogram + churn risk table** | [ ] |
| 6 | **`sellers.py` written — scatter + top 10 + state bar** | [ ] |
| 7 | `ml_insights.py` provided — 3 tabs working | [ ] |
| 8 | `run_dashboard.bat` launches on port 8502 | [ ] |
| 9 | All 5 pages load without error | [ ] |
|10 | One clean `[DAY-047][S07]` commit | [ ] |

---

## 🔍 SELF-CHECK — Key Numbers to Verify

```
Overview page:
  Total Customers: 96,218
  Total Orders:    96,588
  Total Revenue:   ~$13,591,644 (sum of monthly_revenue.total_revenue)
  Avg Review:      ~4.08 ⭐

Orders page (All):
  Total: 96,588
  Late Rate: ~8.1%
  Key insight: 4.29 ⭐ on-time vs 2.57 ⭐ late

Customers page:
  Repeat Rate: ~3.0% (2,886 / 96,218)
  High-value churned: customers with is_churned=1, total_spent > $200

ML Insights:
  Churn tab: histogram peaks near 1.0 (most customers predicted churned)
  Delay tab: histogram peaks near 0 (most orders predicted on-time)
```

---

## 🔜 PREVIEW: DAY 48

**Topic:** Final demo + README + sprint close  
**What you'll do:** Write the capstone README (the document a recruiter reads first),
record a demo walkthrough, create the final architecture diagram,
close Sprint 07 and add `sprint-07-complete` tag.

---

*Day 47 | Sprint 07 | EP-10 | TASK-047*
