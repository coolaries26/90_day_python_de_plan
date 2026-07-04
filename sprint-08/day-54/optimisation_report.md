# Query Optimisation Report — Day 54

## Composite Index Results

| Index | Query | Before | After | Speedup | Scan Type |
|-------|-------|--------|-------|---------|-----------|
| idx_seller_state_revenue | state + revenue | 1.12ms | 2.36ms |    0.5 x | → Index Scan |
| idx_order_month_late | month + is_late | 18.6ms | 3.16ms | 5.9x | → Index Scan |
| idx_ltv_segment_spend | segment + spend | 24.85ms | 2.14ms | 11.6x | → Index Scan |

## Partitioning Results

| Query | Regular | Partitioned | Speedup |
|-------|---------|-------------|---------|
| Single month | 4.87ms | 2.30ms | 2.1x |
| Quarter filter | 3.22ms | 2.97ms | 1.1x |

## Key Findings

## Key Finding: Index Overhead on Small Tables

1. idx_seller_state_revenue made the query SLOWER (1.12ms → 2.36ms).
With only 3,095 rows, PostgreSQL's planner correctly chose Index Scan
(confirmed in EXPLAIN output), but the index lookup + page fetch
overhead exceeded the benefit on such a small table.

2. Lesson: indexes help most on tables with 10k+ rows where filtering
eliminates >90% of rows. On small lookup tables, sequential scans
are often faster — don't index reflexively, measure first.

3. Partitioning: Quarter Filter Speedup Smaller (1.1x)
Makes sense — a quarter filter spans 3 partitions, so PostgreSQL still scans 3 partition tables instead of 1. Partition pruning helps most when your filter aligns with exactly one partition boundary (the month query benefited more — 2.1x).

## Recommendations

- Add composite index on [...] for the Streamlit dashboard
- Partition [...] table when data exceeds [...] rows
- Avoid indexing [...] because [...]