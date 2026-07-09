### TASK T1 — Window Function Query (25 min)

**Brief:** Write a single SQL query (no Python wrapper needed) that answers:

> "For each Brazilian state, find: total sellers, total total_revenue, average review score,
> and rank the state by total total_revenue. Also show what % of national total_revenue each state represents."

**Requirements:**
- Source: `analytics.seller_performance`
- One row per state
- Columns: `seller_state`, `seller_count`, `state_total_revenue`, `avg_review`,
  `total_revenue_rank` (RANK by state_total_revenue DESC),
  `total_revenue_share_pct` (state / national total × 100)
- Use window functions — no subqueries for the rank or share

**Run and save:**
select seller_state,
       count(*) as seller_count,
       sum(total_revenue) as state_total_revenue,
       avg(avg_review_score) as avg_review,
       RANK() OVER (ORDER BY sum(total_revenue) DESC) as total_revenue_rank,
       (sum(total_revenue) / SUM(sum(total_revenue)) OVER()) * 100 as total_revenue_share_pct
from analytics.seller_performance
group by seller_state



```bash
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
df = pd.read_sql('''select seller_state,
       count(*) as seller_count,
       sum(total_revenue) as state_total_revenue,
       avg(avg_review_score) as avg_review,
       RANK() OVER (ORDER BY sum(total_revenue) DESC) as total_revenue_rank,
       (sum(total_revenue) / SUM(sum(total_revenue)) OVER()) * 100 as total_revenue_share_pct
from analytics.seller_performance
group by seller_state
''', engine)
print(df.to_string(index=False))
df.to_csv('sprint-08/day-56/output/t1_state_total_revenue_rank.csv', index=False)
dispose_ecommerce_engine()
"
```

**Pass criteria:**
```
27 rows (one per state)
SP has total_revenue_rank = 1
All total_revenue_share_pct values sum to ~100
```
