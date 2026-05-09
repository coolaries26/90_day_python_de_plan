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
    df=pd.read_sql(
            "SELECT customer_id, total_spend, total_rentals, value_segment FROM analytics_customer_airflow WHERE total_spend IS NOT NULL",
            get_engine()
        )
    fig = px.scatter(
        df,
        x="total_spend",
        y="total_rentals",
        color="value_segment",
        hover_data=["customer_id", "total_spend", "total_rentals"],
        title="Customer Spend vs Rental Count",
        template=PLOTLY_THEME,
        #trendline="ols",    # adds a linear regression line (requires statsmodels)
        trendline="lowess",  # adds a non-parametric smooth curve (also requires statsmodels)
        labels={"total_spend": "Total Spend ($)",
                "total_rentals": "Total Rentals",
                "value_segment": "Segment"},
    )
#    fig = px.scatter(..., trendline="ols")

    save_chart(fig, "p3_spend_vs_rentals")
    return fig


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
    df = pd.read_sql(
            "SELECT category, COUNT(*) AS film_count, ROUND(AVG(rental_rate)::numeric, 2) AS avg_rate, ROUND(SUM(rental_rate)::numeric, 2) AS total_rentals FROM analytics_film_airflow WHERE category IS NOT NULL GROUP BY category ORDER BY total_rentals DESC",
            get_engine()
        )
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

    save_chart(fig, "p4_category_treemap")
    return fig
    


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