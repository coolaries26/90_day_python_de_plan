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
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from plotly_charts import (
    p1_customer_segments as load_customer_segments,
    p2_monthly_revenue as load_monthly_revenue,
    p3_spend_vs_rentals as load_spend_vs_rentals,
    p4_category_treemap as load_category_treemap,
)

PLOTLY_THEME = "plotly_white"

def build_dashboard():
    # Step 1: Create 2x2 subplot grid
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

    # Step 2: Load each chart's data and add traces
    # Panel 1: Customer Segments
    segments_df = load_customer_segments()
    for trace in segments_df.data:
        trace['marker_color'] = 'blue'
        fig.add_trace(trace, row=1, col=1)

    # Panel 2: Monthly Revenue
    revenue_df = load_monthly_revenue()
    for trace in revenue_df.data:
        trace['marker_color'] = 'green'
        fig.add_trace(trace, row=1, col=2)
    

    # Panel 3: Spend vs Rentals
    spend_rentals_df = load_spend_vs_rentals()
    for trace in spend_rentals_df.data:
        trace['marker_color'] = 'orange'
        trace['marker_size'] = 10
        fig.add_trace(trace, row=2, col=1)

    # Panel 4: Category Treemap
    treemap_df = load_category_treemap()
    for trace in treemap_df.data:
        trace['marker_colorscale'] = 'Viridis'
        fig.add_trace(trace, row=2, col=2)

    # Step 3: Update layout
    fig.update_layout(
        title="DVD Rental Analytics Dashboard",
        height=900,
        template=PLOTLY_THEME,
        showlegend=False,
    )

    # Step 4: Save as HTML
    fig.write_html("sprint-05/day-30/output/dashboard.html",
                   include_plotlyjs="cdn")
    print("Dashboard saved → dashboard.html")
    print("Open in Windows browser: file:///C:/90_day_python_de_plan/sprint-05/day-30/output/dashboard.html")

if __name__ == "__main__":
    build_dashboard()   