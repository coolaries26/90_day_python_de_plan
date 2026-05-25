# Capstone Architecture — E-Commerce Analytics Platform

## Dataset
- Source: Olist Brazilian E-Commerce (Kaggle)
- Volume: ~100k orders, 8 tables, 550k total rows
- Period: 2016–2018

## Database Design

### raw schema — direct CSV loads (no transformation)
| Table | Rows | Key Column |
|-------|------|------------|
| orders | 99,441 | order_id |
| order_items | 112,650 | order_id, order_item_id |
| order_payments | 103,886 | order_id |
| order_reviews | 99,224 | review_id |
| customers | 99,441 | customer_id |
| sellers | 3,095 | seller_id |
| products | 32,951 | product_id |
| product_category_translation | 71 | product_category_name |

### analytics schema — derived tables (ETL output)
| Table | Description | Built from |
|-------|-------------|------------|
| customer_ltv | Customer lifetime value + churn flag | orders + payments + customers |
| order_metrics | Delivery time, review score per order | orders + reviews + order_items |
| seller_performance | Revenue, rating, on-time % per seller | order_items + orders + reviews |
| product_analytics | Revenue, volume per category | order_items + products + translation |
| monthly_revenue | Monthly revenue time series + MoM growth | orders + order_payments |

### ml schema — ML outputs
| Table | Description |
|-------|-------------|
| churn_predictions | Customer churn probability |
| delay_predictions | Order delivery delay probability |
| customer_segments | KMeans cluster assignments |

## ML Models
| Model | Target | Features | Algorithm |
|-------|--------|----------|-----------|
| Churn | Has customer ordered again? | LTV, days since last order, review score | RandomForest + SMOTE |
| Delay | Will order be delivered late? | product weight, distance, seller rating | GradientBoosting |

## Pipeline Architecture
[Add your architecture diagram or Mermaid chart here]

## Design Decisions
1. Separate raw/analytics/ml schemas for clean separation
2. appuser NOCREATEDB — uses GRANT CREATE per schema
3. All timestamps stored as TIMESTAMP (not VARCHAR)
4. English category names via JOIN to translation table