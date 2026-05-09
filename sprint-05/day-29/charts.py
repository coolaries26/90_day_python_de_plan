#!/usr/bin/env python3
"""
charts.py — Day 29 | Data Visualization from PostgreSQL
=========================================================
Generates 5 charts from analytics tables.
All charts saved as PNG to sprint-05/day-29/output/.

Run: python charts.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import matplotlib
from pytest_cov import engine
matplotlib.use("Agg")   # non-interactive backend — required for server/WSL
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("charts")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Seaborn theme — applies to all matplotlib/seaborn charts
sns.set_theme(style="whitegrid", palette="husl", font_scale=1.1)
COLORS = sns.color_palette("husl", 8)


def get_engine_local():
    """Return engine — reuse db_utils factory."""
    return get_engine()


# ── C1: Customer Segment Distribution (Bar Chart) ─────────────────────────
def chart_customer_segments() -> Path:
    """
    Bar chart: number of customers per segment (Bronze/Silver/Gold/Platinum).
    Source: analytics_customer_airflow
    """
    engine = get_engine_local()
    df = pd.read_sql("""
        SELECT value_segment, COUNT(*) AS customer_count
        FROM analytics_customer_airflow
        WHERE value_segment IS NOT NULL
        GROUP BY value_segment
        ORDER BY CASE value_segment
            WHEN 'Bronze'   THEN 1
            WHEN 'Silver'   THEN 2
            WHEN 'Gold'     THEN 3
            WHEN 'Platinum' THEN 4
        END
    """, engine)

    fig, ax = plt.subplots(figsize=(8, 5))
    segment_colors = {
        "Bronze":   "#CD7F32",
        "Silver":   "#C0C0C0",
        "Gold":     "#FFD700",
        "Platinum": "#E5E4E2",
    }
    bars = ax.bar(
        df["value_segment"],
        df["customer_count"],
        color=[segment_colors.get(s, COLORS[0]) for s in df["value_segment"]],
        edgecolor="white",
        linewidth=0.8,
    )
    # Add value labels on bars
    for bar, val in zip(bars, df["customer_count"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 2,
            str(val),
            ha="center", va="bottom", fontweight="bold",
        )
    ax.set_title("Customer Segment Distribution", fontsize=14, fontweight="bold")
    ax.set_xlabel("Segment")
    ax.set_ylabel("Number of Customers")
    ax.set_ylim(0, df["customer_count"].max() * 1.15)

    out = OUTPUT_DIR / "c1_customer_segments.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"C1 saved → {out.name}")
    return out


# ── C2: Monthly Revenue Trend (Line Chart) ────────────────────────────────
def chart_monthly_revenue() -> Path:
    """
    Line chart: monthly revenue with month-over-month growth overlay.
    Source: analytics_monthly_enriched
    """
    engine = get_engine_local()
    df = pd.read_sql("""
        SELECT payment_date, total_revenue, mom_growth_pct
        FROM analytics_monthly_enriched
        ORDER BY payment_date
    """, engine)
    df["payment_date"] = pd.to_datetime(df["payment_date"])
    df["month_label"]  = df["payment_date"].dt.strftime("%b %Y")

    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Primary axis — revenue bars
    ax1.bar(
        df["month_label"], df["total_revenue"],
        color=COLORS[0], alpha=0.7, label="Revenue"
    )
    ax1.set_ylabel("Total Revenue ($)", color=COLORS[0])
    ax1.yaxis.set_major_formatter(mtick.FuncFormatter(
        lambda x, _: f"${x:,.0f}"
    ))

    # Secondary axis — MoM growth line
    ax2 = ax1.twinx()
    valid = df["mom_growth_pct"].notna()
    ax2.plot(
        df.loc[valid, "month_label"],
        df.loc[valid, "mom_growth_pct"],
        color=COLORS[3], marker="o", linewidth=2, label="MoM Growth %"
    )
    ax2.set_ylabel("MoM Growth (%)", color=COLORS[3])
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())

    ax1.set_title("Monthly Revenue + MoM Growth", fontsize=14, fontweight="bold")
    ax1.set_xlabel("Month")

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    out = OUTPUT_DIR / "c2_monthly_revenue.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"C2 saved → {out.name}")
    return out


# ── C3: Film Value Tier Distribution (Pie + Bar Combo) ────────────────────
def chart_film_value_tiers() -> Path:
    """
    Horizontal bar chart: film count + average rental rate per value tier.
    Source: analytics_film_airflow
    """
    engine = get_engine_local()
    df = pd.read_sql("""
        SELECT
            value_tier,
            COUNT(*)                        AS film_count,
            ROUND(AVG(rental_rate)::numeric, 2) AS avg_rate
        FROM analytics_film_airflow
        WHERE value_tier IS NOT NULL
        GROUP BY value_tier
        ORDER BY avg_rate DESC
    """, engine)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    tier_colors = ["#E74C3C", "#F39C12", "#27AE60"]

    # Left: film count
    ax1.barh(df["value_tier"], df["film_count"],
             color=tier_colors, edgecolor="white")
    ax1.set_xlabel("Number of Films")
    ax1.set_title("Films per Value Tier")
    for i, (val, tier) in enumerate(zip(df["film_count"], df["value_tier"])):
        ax1.text(val + 3, i, str(val), va="center", fontweight="bold")

    # Right: avg rental rate
    ax2.barh(df["value_tier"], df["avg_rate"],
             color=tier_colors, edgecolor="white")
    ax2.set_xlabel("Avg Rental Rate ($)")
    ax2.set_title("Avg Rate per Value Tier")
    for i, (val, tier) in enumerate(zip(df["avg_rate"], df["value_tier"])):
        ax2.text(float(val) + 0.05, i, f"${val}", va="center", fontweight="bold")

    fig.suptitle("Film Value Tier Analysis", fontsize=14, fontweight="bold")

    out = OUTPUT_DIR / "c3_film_value_tiers.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"C3 saved → {out.name}")
    return out


# ── C4: Pipeline Run History (Heatmap) — WRITE THIS YOURSELF ─────────────
def chart_pipeline_history() -> Path:
    """
    C4 — YOUR TASK:
    Heatmap showing pipeline run counts by pipeline_name and status.
    Source: etl_audit_log

    HINTS:
    Step 1: Load data
        df = pd.read_sql(
            "SELECT pipeline_name, status, COUNT(*) AS run_count
             FROM etl_audit_log
             GROUP BY pipeline_name, status",
            engine
        )

    Step 2: Pivot for heatmap
        pivot = df.pivot(
            index="pipeline_name",
            columns="status",
            values="run_count"
        ).fillna(0)

    Step 3: Plot seaborn heatmap
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(
            pivot,
            annot=True,      # show values in cells
            fmt=".0f",       # integer format
            cmap="YlOrRd",   # yellow-orange-red colormap
            linewidths=0.5,
            ax=ax,
        )
        ax.set_title("Pipeline Run History by Status")
        ax.set_xlabel("Status")
        ax.set_ylabel("Pipeline")

    Step 4: Save to OUTPUT_DIR / "c4_pipeline_history.png"
    Step 5: Return path
    """
    # YOUR CODE HERE
    engine = get_engine_local()
    sql = """
        SELECT pipeline_name, status, COUNT(*) AS run_count
             FROM etl_audit_log
             GROUP BY pipeline_name, status
    """
    df = pd.read_sql(sql, engine)
    pivot = df.pivot(index="pipeline_name", columns="status", values="run_count").fillna(0)

    fig, ax = plt.subplots(figsize=(8, 6))  # Adjust the figure size
    sns.heatmap(
        pivot,
        annot=True,      # show values in cells
        fmt=".0f",       # integer format
        cmap="YlOrRd",   # yellow-orange-red colormap
        linewidths=0.5,
        ax=ax,
    )
    ax.set_title("Pipeline Run History by Status")
    ax.set_xlabel("Status")
    ax.set_ylabel("Pipeline")

    out = OUTPUT_DIR / "c4_pipeline_history.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"C4 saved → {out.name}")
    return out  


# ── C5: Customer Spend Distribution (Histogram + KDE) — WRITE THIS ────────
def chart_customer_spend_distribution() -> Path:
    """
    C5 — YOUR TASK:
    Histogram with KDE overlay showing distribution of customer total spend.
    Source: analytics_customer_airflow

    HINTS:
    Step 1: Load data
        df = pd.read_sql(
            "SELECT total_spend FROM analytics_customer_airflow
             WHERE total_spend IS NOT NULL",
            engine
        )

    Step 2: Plot using seaborn histplot with KDE
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.histplot(
            data=df,
            x="total_spend",
            bins=30,
            kde=True,            # overlay KDE curve
            color=COLORS[1],
            ax=ax,
        )

    Step 3: Add vertical lines for mean and median
        mean   = df["total_spend"].mean()
        median = df["total_spend"].median()
        ax.axvline(mean,   color="red",    linestyle="--", label=f"Mean: ${mean:.2f}")
        ax.axvline(median, color="orange", linestyle="--", label=f"Median: ${median:.2f}")
        ax.legend()

    Step 4: Labels, title, save
        ax.set_title("Customer Total Spend Distribution")
        ax.set_xlabel("Total Spend ($)")
        ax.set_ylabel("Count")

    Step 5: Save to OUTPUT_DIR / "c5_customer_spend_dist.png"
    """
    # YOUR CODE HERE
    engine = get_engine_local()
    sql = """
        SELECT total_spend FROM analytics_customer_airflow
        WHERE total_spend IS NOT NULL
    """
    df = pd.read_sql(sql, engine)

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.histplot(
        data=df,
        x="total_spend",
        bins=30,
        kde=True,            # overlay KDE curve
        color=COLORS[1],
        ax=ax,
    )
    mean   = df["total_spend"].mean()
    median = df["total_spend"].median()
    ax.axvline(mean,   color="red",    linestyle="--", label=f"Mean: ${mean:.2f}")
    ax.axvline(median, color="orange", linestyle="--", label=f"Median: ${median:.2f}")
    ax.legend()
    ax.set_title("Customer Total Spend Distribution")
    ax.set_xlabel("Total Spend ($)")    
    ax.set_ylabel("Count")
    out = OUTPUT_DIR / "c5_customer_spend_dist.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"C5 saved → {out.name}")
    return out




# ── Runner ────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("=" * 52)
    logger.info("Chart Generation — Day 29")
    logger.info("=" * 52)

    charts = [
        ("C1: Customer Segments",        chart_customer_segments),
        ("C2: Monthly Revenue",          chart_monthly_revenue),
        ("C3: Film Value Tiers",         chart_film_value_tiers),
        ("C4: Pipeline History Heatmap", chart_pipeline_history),
        ("C5: Customer Spend Dist",      chart_customer_spend_distribution),
    ]

    generated = []
    for label, fn in charts:
        try:
            path = fn()
            generated.append(path)
            logger.info(f"✅ {label}")
        except NotImplementedError as e:
            logger.warning(f"⏳ {label} — {e}")
        except Exception as e:
            logger.error(f"❌ {label} — {e}")

    logger.info(f"\n{len(generated)}/5 charts generated")
    logger.info(f"Output: {OUTPUT_DIR}")
    dispose_engine()


if __name__ == "__main__":
    main()