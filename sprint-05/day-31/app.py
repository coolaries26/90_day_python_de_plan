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
# In app.py sidebar section, add:

    # Manual refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()   # clears ALL cached data
        st.rerun()              # re-runs the entire script

    st.caption(f"Cache TTL: 5 min")
    st.caption("Python DE Journey — Day 32")
    st.caption("Data: dvdrental PostgreSQL")

# Load selected page
if page == "📊 Overview":
    import pages.overview as overview
    overview.render()
elif page == "👥 Customers":
    import pages.customers as customers
    customers.render()
elif page == "🎬 Films":
#    st.title("🎬 Films")
    import pages.films as films
    films.render()
else:
    st.info("Films page coming on Day 32")