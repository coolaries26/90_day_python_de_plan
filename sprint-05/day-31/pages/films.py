"""
films.py — Film Analytics Page
================================
YOUR TASK: Build the films analytics page.

Requirements:
1. Page title: "🎬 Film Analytics"

2. Sidebar filters:
   - Value tier multiselect: ["Budget", "Standard", "Premium"]
     default = all three
   - Rating multiselect: ["G", "PG", "PG-13", "R", "NC-17"]
     default = all
   - Min value score slider: 0.0 to 100.0, step 1.0

3. KPI row (3 metrics):
   - Filtered Films count
   - Avg Rental Rate (mean of filtered, rounded to 2dp, prefix "$")
   - Avg Value Score (mean of filtered, rounded to 1dp)

4. Two charts side by side (use st.columns(2)):
   Left:  Bar chart — film count by value_tier (filtered)
          px.bar(x="value_tier", y="count", color="value_tier")

   Right: Box plot — rental_rate distribution by rating (filtered)
          px.box(x="rating", y="rental_rate", color="rating")
          title="Rental Rate by Rating"

5. Top 20 films table (filtered, sorted by value_score DESC):
   columns: [title, category, rating, value_tier, rental_rate, value_score]
   st.dataframe(..., use_container_width=True)

HINTS:
  from db import load_films
  df = load_films()

  # Sidebar filters (add BELOW app navigation in sidebar)
  with st.sidebar:
      st.markdown("---")
      tiers   = st.multiselect("Value Tier", ["Budget","Standard","Premium"],
                               default=["Budget","Standard","Premium"])
      ratings = st.multiselect("Rating", ["G","PG","PG-13","R","NC-17"],
                               default=["G","PG","PG-13","R","NC-17"])
      min_score = st.slider("Min Value Score", 0.0, 100.0, 0.0, step=1.0)

  filtered = df[
      (df["value_tier"].isin(tiers)) &
      (df["rating"].isin(ratings)) &
      (df["value_score"] >= min_score)
  ]

  # Charts in columns
  col1, col2 = st.columns(2)
  with col1:
      tier_counts = (filtered.groupby("value_tier")
                     .size().reset_index(name="count"))
      fig1 = px.bar(...)
      st.plotly_chart(fig1, use_container_width=True)

  with col2:
      fig2 = px.box(...)
      st.plotly_chart(fig2, use_container_width=True)

Self-check:
  - All 3 pages load without error
  - Filter to Premium only → ~336 films
  - Filter to G rating + all tiers → varies
  - Value score slider at 90 → small subset of highest-scoring films
"""
# YOUR CODE HERE
import pandas as pd
import streamlit as st
import plotly.express as px
from db import load_films
import pandas as pd

def render():
    st.title("🎬 Film Analytics")
    st.markdown("---")
    df = load_films()
    # In sidebar, BEFORE the tier multiselect:
    with st.sidebar:
        st.markdown("---")
        search = st.text_input("🔍 Search by Title", placeholder="e.g. academy")

        tiers   = st.multiselect("Value Tier", ["Budget","Standard","Premium"],
                                 default=["Budget","Standard","Premium"])
        ratings = st.multiselect("Rating", ["G","PG","PG-13","R","NC-17"],
                                 default=["G","PG","PG-13","R","NC-17"])
        min_score = st.slider("Min Value Score", 0.0, 100.0, 0.0, step=1.0)
    
    # Apply search filter FIRST, then other filters:
    filtered = df.copy()
    if search:
        filtered = filtered[
            filtered["title"].str.contains(search, case=False, na=False)
        ]
    filtered = filtered[
        (filtered["value_tier"].isin(tiers)) &
        (filtered["rating"].isin(ratings)) &
        (filtered["value_score"] >= min_score)
    ]

    # Show search result count
    if search:
        st.caption(f"🔍 '{search}' matched {len(filtered)} films")

#     filtered = df.query(
#         "value_tier in @tiers and "
#         "rating in @ratings and "
#         "value_score >= @min_score"
#     )
    # KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Filtered Films", len(filtered))
#    st.write("filtered dtype:", filtered.dtype)
#    st.write("Sample values:", filtered[ "value_score"].head())
    col2.metric("Avg Rental Rate", f"{str(round(filtered['rental_rate'].mean(), 2))}")
    col3.metric("Avg Value Score", f"{filtered['value_score'].mean():.1f}")
    # Charts in columns
    col1, col2 = st.columns(2)
    with col1:
        tier_counts = (filtered.groupby("value_tier")
                       .size().reset_index(name="count"))
        fig1 = px.bar(tier_counts, x="value_tier", y="count", color="value_tier",
                      title="Film Count by Value Tier")
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        fig2 = px.box(filtered, x="rating", y="rental_rate", color="rating",
                      title="Rental Rate by Rating")
        st.plotly_chart(fig2, use_container_width=True)
    # Top 20 films table
#    top_films = (filtered.sort_values("value_tier", ascending=False)
#                 .head(20))
#    st.dataframe(top_films, use_container_width=True)
#    st.markdown("---")
#    st.subheader("*Note: 'full_name' column may not be available if not included in the analytics table.*")
    st.markdown("---")
# Add at bottom of each page, ABOVE the download button:
    with st.expander("📋 View Raw Data", expanded=False):
        st.caption(f"{len(filtered):,} rows × {len(filtered.columns)} columns")
        st.dataframe(filtered, use_container_width=True, height=300)

# ── CSV Export ────────────────────────────────────────────────────────────
    st.markdown("---")
    col_dl, col_info = st.columns([1, 3])
    with col_dl:
        csv_bytes = filtered.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download CSV",
            data=csv_bytes,
            file_name=f"customers_filtered_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_info:
        st.caption(f"Downloading {len(filtered):,} rows × {len(filtered.columns)} columns")    