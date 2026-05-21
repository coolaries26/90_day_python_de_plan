"""
app.py — DVD Rental Analytics Dashboard
========================================
Main entry point for the Streamlit app.
Run: streamlit run sprint-05/day-31/app.py

Navigation: sidebar radio buttons → loads different pages
"""

import streamlit as st
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent / "pages"))    # for page imports

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
        options=[
            "📊 Overview", 
            "👥 Customers", 
            "🎬 Films", 
            "📅 Rentals", 
            "⚠️ Churn Risk",
            "🔵 Customer Clusters"
            ],
        index=0,
    )
    
    st.markdown("---")
    st.caption("Python DE Journey — Day 31")
    st.caption("Data: dvdrental PostgreSQL")
# In app.py sidebar section, add:

    # Manual refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()   # clears ALL cached data
        st.rerun()              # re-runs the entire script

    st.caption(f"Cache TTL: 5 min")
    st.caption("Python DE Journey — Day 32")
    st.caption("Data: dvdrental PostgreSQL")

# Page imports
# Load selected page
if page == "📊 Overview":
    import overview as overview
    overview.render()
elif page == "👥 Customers":
    import customers as customers
    customers.render()
elif page == "🎬 Films":
    import films as films
    films.render()
elif page == "📅 Rentals":  # This page has been added on Day 35
    import rentals as rentals
    rentals.render()
elif page == "⚠️ Churn Risk": # This page has been added on Day 37 for churn risk analytics
    import churn_risk as churn
    churn.render()
elif page == "🔵 Customer Clusters":    # This page has been added on Day 41 for customer clustering
    import clusters as clusters
    clusters.render()
else:
    st.info("Films page coming on Day 32")
