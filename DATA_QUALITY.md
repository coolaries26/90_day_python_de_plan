## Column Naming Inconsistency

Table                              Column Name
analytics.order_metrics            delivery_days_actual
dbt_dev_marts.mart_order_metrics   delivery_days

Both contain the same data (actual days from purchase to delivery).
The Streamlit dashboard uses `delivery_days` (dbt mart version).
If switching back to analytics schema, update queries accordingly.

TODO: Standardise on `delivery_days` in a future analytics_etl.py update.

Two Notes
T2 — 2,970 rows not 3,095:
Your mart_seller_summary joined order_items to sellers but 125 sellers have no orders in the dataset (they're registered but never fulfilled an order). Both are defensible:

3,095 = all registered sellers (LEFT JOIN from sellers)
2,970 = only sellers with at least one order (INNER JOIN)

For business reporting, 2,970 is actually more useful — you only care about active sellers.
T3 — 0.9x speedup (index overhead again):
Same pattern as Day 54 Index 1. With only 3,095 rows, the planner switched to Index Scan but the overhead made it marginally slower. You now have two data points confirming the same principle — this is a consistent finding, not a fluke.

