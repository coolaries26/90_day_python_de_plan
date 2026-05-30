"""
overview.py — Overview page
YOUR TASK: Build the overview page.

Requirements:
1. Title: "📊 Business Overview"
2. 4 KPI cards (use st.columns(4)):
   - Total Customers
   - Total Orders
   - Total Revenue (format as $X,XX,XX.XX)
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
import streamlit as st
from pathlib import Path
import sys
PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PATH))
#print(f"DEBUG: Added {PATH} to sys.path")
from an_db import get_kpis, load_monthly_revenue, load_product_analytics
#from db import get_commerce_engine
import plotly.express as px
import plotly.graph_objects as go
def render():
    st.title("📊 Business Overview")
    col1, col2, col3 = st.columns(3)
    kpis = get_kpis()
    col1.metric("Total Customers", f"{kpis['total_customers']:,}")
    col2.metric("Total Orders", f"{kpis['total_orders']:,}")
    col3.metric("Total Revenue", f"${kpis['total_revenue']:,.0f}")
    col4, col5, col6, col7, col8 = st.columns(5)
    col5.metric("Late Delivery Rate", f"{kpis['late_delivery_rate']*100:.2f}%")
    col6.metric("Repeat Customers", f"{kpis['repeat_customers']:,}")
    col7.metric("Avg Customer LTV", f"${kpis['avg_ltv']:,.2f}")
    col8.metric("churn rate", f"{kpis['churn_rate']*100:.2f}%")
    col4.metric("Avg Review", f"{kpis['avg_review_score']:.2f} ⭐")
    try:
        monthly = load_monthly_revenue()
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly["order_month"], y=monthly["total_revenue"]))
        fig.add_trace(go.Scatter(x=monthly["order_month"], y=monthly["total_orders"], mode="lines+markers", name="Order Count", yaxis="y2"))
        fig.update_layout(barmode="group")
        st.plotly_chart(fig)
    except Exception as e:
        st.error(f"Error loading monthly revenue data: {e}")

    try:
        product = load_product_analytics()
        top5 = product.head(5)
        fig2 = px.bar(top5, x="total_revenue", y="product_category_name_english", orientation="h", title="Top 5 Product Categories by Revenue")
        st.plotly_chart(fig2)
    except Exception as e:
        st.error(f"Error loading product analytics data: {e}")
