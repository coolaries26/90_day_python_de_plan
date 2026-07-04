## Column Naming Inconsistency

Table                              Column Name
analytics.order_metrics            delivery_days_actual
dbt_dev_marts.mart_order_metrics   delivery_days

Both contain the same data (actual days from purchase to delivery).
The Streamlit dashboard uses `delivery_days` (dbt mart version).
If switching back to analytics schema, update queries accordingly.

TODO: Standardise on `delivery_days` in a future analytics_etl.py update.
