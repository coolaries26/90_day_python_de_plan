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