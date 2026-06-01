# Data Quality Findings

## Known Issues

| Issue | Table | Impact | Resolution |
|-------|-------|--------|------------|
| Multiple rows per order_id | raw.order_payments | Double-counting revenue | GROUP BY + SUM before JOIN |
| NULL delivery dates | raw.orders | ~3k undelivered orders | Filter WHERE status='delivered' |
| Portuguese category names | raw.products | Unreadable in reports | JOIN to product_category_translation |
| is_churned=100% with time-based definition | analytics.customer_ltv | Unusable for ML | Redefined as single-purchase |
| delay_pipeline overestimates late rate | ml.delay_predictions | 52% vs actual 8% | Weak features; model limitation documented |
| monthly_revenue has 22 not 24 months | analytics.monthly_revenue | Minor | Boundary months incomplete |
| Delay predicted rate 51.9% | ⚠️ Note | Document as model limitation | 

## Column Notes
- review_comment_message: contains raw HTML — not cleaned (not used in analytics)
- order_approved_at: ~160 NULLs (orders not approved) — excluded from delivery calc
- product_weight_g: ~600 NULLs — imputed with category median in future work

## Churn Definition Decision
Single-purchase definition chosen over time-based because:
  - Dataset ends Aug 2018 — all customers appear inactive from today's date
  - Single-purchase is objective and consistent with Olist's known 97% one-time buyer rate
  - Industry benchmark: 95-97% single purchase is normal for Brazilian e-commerce (2017-2018)
```

---
