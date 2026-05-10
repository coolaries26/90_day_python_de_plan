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
import streamlit as st
import plotly.express as px 
from db import load_customers

def render():
    st.title("👥 Customer Analytics")
    st.markdown("---")
    st.markdown("Explore customer segments, spending, and rental behaviour. Use the filters in the sidebar to refine the analysis.")
    st.markdown("---")

    # Sidebar filters
    with st.sidebar:
        st.header("Filter Customers")
        segments = st.multiselect(
            "Segments",
            options=["Bronze", "Silver", "Gold", "Platinum"],
            default=["Bronze", "Silver", "Gold", "Platinum"]
        )
        min_spend = st.slider("Min Spend ($)", 0, 250, 0)
    
    filtered = load_customers()[
        (load_customers()["value_segment"].isin(segments)) &
        (load_customers()["total_spend"] >= min_spend)
    ]
    
    st.metric("Filtered Customers", len(filtered))
    
    fig_seg = px.bar(
        filtered.groupby("value_segment").size().reset_index(name="count"),
        x="value_segment", y="count", color="value_segment",
        title="Customers by Segment (filtered)"
    )
    st.plotly_chart(fig_seg, use_container_width=True)
    fig_scatter = px.scatter(
        filtered,
        x="total_spend", y="total_rentals",
        color="value_segment",
        title="Total Spend vs Total Rentals (filtered)"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)
    
    cols = ["customer_id", "full_name", "value_segment", "total_spend", "total_rentals"]
    st.dataframe(filtered[cols], use_container_width=True)
    st.markdown("---")
    st.subheader("*Note: 'full_name' column may not be available if not included in the analytics table.*")
