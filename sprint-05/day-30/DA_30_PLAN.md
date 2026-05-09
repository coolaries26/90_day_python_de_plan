# 📅 DAY 30 — Sprint 05 | Plotly Interactive Charts
## Interactive HTML Charts + Subplots + Export for Streamlit

---

## 🔁 RETROSPECTIVE — Day 29

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| 5/5 charts generated | ✅ Pass | All >40KB |
| `value_segment` fix | ✅ Pass | Correct column name identified independently |
| matplotlib Agg backend | ✅ Pass | No display errors |
| chart_utils.py | ✅ Pass | |
| [DAY-029][S05] commit | ✅ Pass | Clean |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-05/day-30-plotly
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-07: Data Visualization & Reporting                        |
| Story           | ST-30: Plotly Interactive Charts + HTML Export               |
| Task ID         | TASK-030                                                     |
| Sprint          | Sprint 05 (Days 29–35)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | plotly, interactive, html, charts, day-30                    |
| Acceptance Criteria | 4 interactive Plotly charts; saved as HTML + PNG; subplot dashboard created; importable by Streamlit |

---

## 📚 BACKGROUND

### Plotly vs Matplotlib

```
matplotlib/seaborn          plotly
──────────────────────────  ──────────────────────────────────
Static PNG                  Interactive HTML
No hover                    Hover tooltips on every data point
No zoom                     Pan/zoom built in
Good for reports/Airflow    Good for dashboards/web
Code: plt.bar(x, y)         Code: px.bar(df, x="col", y="col")
```

### Plotly Express vs Plotly Graph Objects

```python
import plotly.express as px          # high-level, one-liner charts
import plotly.graph_objects as go    # low-level, full control
from plotly.subplots import make_subplots  # multi-panel layouts

# Express — fast, automatic layout:
fig = px.bar(df, x="segment", y="count", title="Customers")
fig.show()   # opens browser
fig.write_html("chart.html")   # save as interactive HTML
fig.write_image("chart.png")   # save as static PNG (needs kaleido)

# Graph Objects — when you need precise control:
fig = go.Figure()
fig.add_trace(go.Bar(x=df["x"], y=df["y"], name="Revenue"))
fig.add_trace(go.Scatter(x=df["x"], y=df["y2"], name="Growth"))
fig.update_layout(title="Revenue + Growth")
```

### Why Both HTML and PNG

```
HTML  → Streamlit (Day 31+): st.plotly_chart(fig) renders interactive
PNG   → Airflow reports: embed in Markdown audit reports
      → Email attachments: static image
```

---

## 🎯 OBJECTIVES

1. Rebuild C1 (segments) and C2 (monthly revenue) as interactive Plotly charts
2. Build C6: Customer spend vs rental count scatter with hover details
3. Build C7: Film category revenue treemap
4. Create a 4-panel subplot dashboard
5. Export all as HTML + PNG
6. Push clean `[DAY-030][S05]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                            |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Branch setup + plotly verify                        |
| B     | 40 min   | `plotly_charts.py` — P1-P2 provided, P3-P4 yours   |
| C     | 30 min   | `dashboard_subplot.py` — 4-panel layout             |
| D     | 20 min   | Export HTML + PNG verification                      |
| E     | 20 min   | Git push                                            |

---

## 📝 EXERCISES

---

### EXERCISE 1 — plotly_charts.py (Block B)
**[P1-P2 fully provided. P3-P4 write yourself]**

Create `sprint-05/day-30/plotly_charts.py`:

```python
#!/usr/bin/env python3
"""
plotly_charts.py — Day 30 | Interactive Plotly Charts
======================================================
Rebuilds matplotlib charts as interactive HTML.
Adds two new chart types: scatter and treemap.

Run: python plotly_charts.py
Output: sprint-05/day-30/output/*.html + *.png
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("plotly_charts")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

PLOTLY_THEME = "plotly_white"


def save_chart(fig: go.Figure, name: str) -> tuple[Path, Path]:
    """Save figure as both HTML and PNG. Returns (html_path, png_path)."""
    html_path = OUTPUT_DIR / f"{name}.html"
    png_path  = OUTPUT_DIR / f"{name}.png"

    fig.write_html(str(html_path), include_plotlyjs="cdn")
    try:
        fig.write_image(str(png_path), width=1000, height=600, scale=1.5)
        logger.info(f"Saved {name} → HTML + PNG")
    except Exception as exc:
        logger.warning(f"PNG export failed for {name} (kaleido issue?): {exc}")
        logger.info(f"Saved {name} → HTML only")

    return html_path, png_path


# ── P1: Customer Segment Bar (Interactive) — provided ────────────────────
def p1_customer_segments() -> go.Figure:
    """Interactive bar chart — customer counts per segment with hover."""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT value_segment AS segment,
               COUNT(*) AS customer_count,
               ROUND(AVG(total_spend)::numeric, 2) AS avg_spend
        FROM analytics_customer_airflow
        WHERE value_segment IS NOT NULL
        GROUP BY value_segment
        ORDER BY CASE value_segment
            WHEN 'Bronze' THEN 1 WHEN 'Silver' THEN 2
            WHEN 'Gold'   THEN 3 WHEN 'Platinum' THEN 4
        END
    """, engine)

    fig = px.bar(
        df,
        x="segment",
        y="customer_count",
        color="segment",
        text="customer_count",
        hover_data={"avg_spend": True, "customer_count": True},
        color_discrete_map={
            "Bronze": "#CD7F32", "Silver": "#C0C0C0",
            "Gold": "#FFD700",   "Platinum": "#E5E4E2",
        },
        title="Customer Segment Distribution",
        template=PLOTLY_THEME,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        xaxis_title="Segment",
        yaxis_title="Number of Customers",
        showlegend=False,
        hoverlabel=dict(bgcolor="white"),
    )
    save_chart(fig, "p1_customer_segments")
    return fig


# ── P2: Monthly Revenue Line — provided ──────────────────────────────────
def p2_monthly_revenue() -> go.Figure:
    """Dual-axis interactive chart: revenue bars + MoM growth line."""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT payment_date, total_revenue, payment_count, mom_growth_pct
        FROM analytics_monthly_enriched
        ORDER BY payment_date
    """, engine)
    df["payment_date"] = pd.to_datetime(df["payment_date"])
    df["month"] = df["payment_date"].dt.strftime("%b %Y")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=df["month"], y=df["total_revenue"],
            name="Revenue ($)",
            marker_color="steelblue",
            opacity=0.8,
            hovertemplate="Month: %{x}<br>Revenue: $%{y:,.2f}<extra></extra>",
        ),
        secondary_y=False,
    )
    valid = df["mom_growth_pct"].notna()
    fig.add_trace(
        go.Scatter(
            x=df.loc[valid, "month"],
            y=df.loc[valid, "mom_growth_pct"],
            name="MoM Growth (%)",
            mode="lines+markers",
            line=dict(color="tomato", width=2),
            marker=dict(size=8),
            hovertemplate="Month: %{x}<br>Growth: %{y:.1f}%<extra></extra>",
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title="Monthly Revenue + MoM Growth",
        template=PLOTLY_THEME,
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
    fig.update_yaxes(title_text="MoM Growth (%)", secondary_y=True)

    save_chart(fig, "p2_monthly_revenue")
    return fig


# ── P3: Customer Spend vs Rental Count Scatter — WRITE YOURSELF ───────────
def p3_spend_vs_rentals() -> go.Figure:
    """
    P3 — YOUR TASK:
    Scatter plot: customer total_spend (x) vs total_rentals (y).
    Each point = one customer, coloured by value_segment.
    Hover shows: customer full_name (or customer_id), spend, rentals, segment.

    HINTS:
    Step 1: Load data
        df = pd.read_sql(
            "SELECT customer_id, total_spend, total_rentals, value_segment
             FROM analytics_customer_airflow
             WHERE total_spend IS NOT NULL",
            engine
        )

    Step 2: Use px.scatter()
        fig = px.scatter(
            df,
            x="total_spend",
            y="total_rentals",
            color="value_segment",
            hover_data=["customer_id", "total_spend", "total_rentals"],
            title="Customer Spend vs Rental Count",
            template=PLOTLY_THEME,
            labels={"total_spend": "Total Spend ($)",
                    "total_rentals": "Total Rentals",
                    "value_segment": "Segment"},
        )

    Step 3: Add trendline (optional but impressive)
        fig = px.scatter(..., trendline="ols")
        # requires: pip install statsmodels

    Step 4: save_chart(fig, "p3_spend_vs_rentals")
    Step 5: return fig

    Self-check: 599 points visible, 4 colours for segments
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement p3_spend_vs_rentals")


# ── P4: Film Category Revenue Treemap — WRITE YOURSELF ───────────────────
def p4_category_treemap() -> go.Figure:
    """
    P4 — YOUR TASK:
    Treemap showing film categories sized by rental count.
    Each rectangle = one category, size = number of films, colour = avg rental rate.

    HINTS:
    Step 1: Load data from analytics_film_airflow
        df = pd.read_sql(
            "SELECT category, COUNT(*) AS film_count,
                    ROUND(AVG(rental_rate)::numeric, 2) AS avg_rate,
                    SUM(rental_count) AS total_rentals
             FROM analytics_film_airflow
             WHERE category IS NOT NULL
             GROUP BY category
             ORDER BY total_rentals DESC",
            engine
        )

    Step 2: Use px.treemap()
        fig = px.treemap(
            df,
            path=["category"],           # hierarchy levels
            values="film_count",         # rectangle size
            color="avg_rate",            # rectangle colour
            color_continuous_scale="RdYlGn",
            hover_data=["total_rentals", "avg_rate"],
            title="Film Categories by Count and Avg Rental Rate",
            template=PLOTLY_THEME,
        )

    Step 3: save_chart(fig, "p4_category_treemap")
    Step 4: return fig

    Self-check: 16 rectangles (one per category in dvdrental)
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement p4_category_treemap")


# ── Runner ────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("=" * 52)
    logger.info("Plotly Charts — Day 30")
    logger.info("=" * 52)

    charts = [
        ("P1: Customer Segments", p1_customer_segments),
        ("P2: Monthly Revenue",   p2_monthly_revenue),
        ("P3: Spend vs Rentals",  p3_spend_vs_rentals),
        ("P4: Category Treemap",  p4_category_treemap),
    ]

    figures = {}
    for label, fn in charts:
        try:
            fig = fn()
            figures[label] = fig
            logger.info(f"✅ {label}")
        except NotImplementedError as e:
            logger.warning(f"⏳ {label} — {e}")
        except Exception as e:
            logger.error(f"❌ {label} — {e}")

    logger.info(f"\n{len(figures)}/4 charts generated")
    dispose_engine()
    return figures


if __name__ == "__main__":
    main()
```

---

### EXERCISE 2 — dashboard_subplot.py: 4-Panel Layout (Block C)
**[Write yourself — hints given]**

Create `sprint-05/day-30/dashboard_subplot.py`:

```python
"""
dashboard_subplot.py — Day 30 | 4-Panel Plotly Dashboard
=========================================================
Combines all 4 charts into one interactive subplot layout.
This becomes the basis for the Streamlit dashboard on Day 31.

YOUR TASK: Implement build_dashboard()

HINTS:
Step 1: Create 2x2 subplot grid
    from plotly.subplots import make_subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "Customer Segments", "Monthly Revenue",
            "Spend vs Rentals",  "Category Treemap"
        ],
        specs=[
            [{"type": "bar"},    {"type": "bar"}],
            [{"type": "scatter"},{"type": "treemap"}],
        ]
    )

Step 2: Load each chart's data and add traces
    For each panel — load data, create go.Bar/go.Scatter/go.Treemap
    Add with fig.add_trace(..., row=1, col=1)

    Note: px charts can't be added to subplots directly.
    Use go.Bar, go.Scatter, go.Treemap instead.

Step 3: Update layout
    fig.update_layout(
        title="DVD Rental Analytics Dashboard",
        height=900,
        template=PLOTLY_THEME,
        showlegend=False,
    )

Step 4: Save as HTML
    fig.write_html("sprint-05/day-30/output/dashboard.html",
                   include_plotlyjs="cdn")
    print("Dashboard saved → dashboard.html")
    print("Open in Windows browser: file:///C:/90_day_python_de_plan/sprint-05/day-30/output/dashboard.html")

Self-check: Open dashboard.html in browser
  → 4 panels visible, all interactive, zoom/hover working
"""
# YOUR CODE HERE
```

---

### EXERCISE 3 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 30 --sprint 5 ^
    --message "Plotly: 4 interactive charts HTML+PNG, 4-panel subplot dashboard" ^
    --merge
```

---

## ✅ DAY 30 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | P1/P2 run and save HTML + PNG                                            | [ ]   |
| 2 | **P3: Scatter spend vs rentals — 599 points, 4 colours**                 | [ ]   |
| 3 | **P4: Category treemap — 16 rectangles**                                 | [ ]   |
| 4 | All 4 HTML files openable in browser with hover/zoom working             | [ ]   |
| 5 | **`dashboard_subplot.py` written — 4-panel layout**                      | [ ]   |
| 6 | `dashboard.html` opens in browser with all 4 panels                     | [ ]   |
| 7 | One clean `[DAY-030][S05]` commit via `daily_commit.py --merge`          | [ ]   |

---

## ⚠️ WATCH OUT FOR

**PNG export on Windows:**
`kaleido` sometimes fails silently on Windows. If `write_image()` raises an error:
```bash
pip install --upgrade kaleido
# If still failing, skip PNG and HTML-only is acceptable for Day 30
```

**Treemap with subplots:**
`px.treemap()` returns a Figure with a `Treemap` trace. To add it to a subplot:
```python
# Extract the trace from the px figure and add it:
px_treemap = px.treemap(df, ...)
treemap_trace = px_treemap.data[0]
fig.add_trace(treemap_trace, row=2, col=2)
```

**dashboard.html file path on Windows:**
The HTML file is in WSL2 path `/mnt/c/...` but you open it in Windows browser at:
```
file:///C:/90_day_python_de_plan/sprint-05/day-30/output/dashboard.html
```

---

## 🔜 PREVIEW: DAY 31

**Topic:** Streamlit app skeleton + PostgreSQL live connection  
**What you'll do:** Install Streamlit, build the app scaffold with sidebar navigation,
connect to PostgreSQL via `st.connection()`, display your Plotly charts
as interactive widgets. The full dashboard is built across Days 31–33.

---

*Day 30 | Sprint 05 | EP-07 | TASK-030*