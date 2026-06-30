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

1. [Your finding about when indexes help most]
2. [Your finding about composite index column order]
3. [Your finding about partitioning on small datasets]

## Recommendations

- Add composite index on [...] for the Streamlit dashboard
- Partition [...] table when data exceeds [...] rows
- Avoid indexing [...] because [...]