# Data Quality Findings — Day 03

| Table   | Column         | Issue Type      | Detail                        | Recommended Fix        |
|---------|----------------|-----------------|-------------------------------|------------------------|
| rental  | return_date    | Expected nulls  | 183 rows null = open rentals  | Filter in pipeline     |
| customer| active         | Wrong dtype     | int64 should be boolean       | Cast in T1 transform   |
| payment | amount         | Precision risk  | float64 for monetary data     | Use Decimal or integer |
| address | address2       | Expected Nulls  | 4 rows null                   | Filter in pipeline     |
| staff   | picture        | Expected Nulls  | 1 rows null                   | Filter in pipeline     |
| analytics_monthly_cohort     | mom_growth_pct| Expected Nulls  |1 rows null                  | Filter in pipeline     |
