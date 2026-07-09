### TASK T3 — Index + EXPLAIN (15 min)

**Brief:** Without referencing Day 54, add an index that speeds up this query:

```sql
SELECT seller_id, total_revenue, avg_review_score
FROM analytics.seller_performance
WHERE on_time_delivery_rate >= 0.9
  AND total_revenue > 5000
ORDER BY total_revenue DESC
```

**Requirements:**
1. Measure time BEFORE index
2. Create the index (choose appropriate columns)
3. Measure time AFTER index
4. Print speedup

```bash
python sprint-08/day-56/t3_index_test.py
```

**Pass criteria:**
```
Index created without error
Before/after timing printed
Speedup calculated (any value — measuring is what matters)
```

---
