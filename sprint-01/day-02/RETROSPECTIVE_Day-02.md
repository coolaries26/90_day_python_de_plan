# 🔁 Day 02 Retrospective
## Sprint 01 | PostgreSQL + Python Foundations

> Complete this at the END of Day 02 (after all exercises).
> Honest reflection drives faster improvement.

---

## ✅ What Went Well

- [✅] db_utils.py created and imports cleanly.
- [✅] db_explorer.py successfully discovered all 15 tables.
- [✅] schema_report.md generated with full schema details.
- [✅] Q1 query executed correctly (top 10 films).
- [✅] Q2 query executed correctly (revenue by rating).
- [✅] Q3 query executed correctly (monthly trends).
- [✅] Q4–Q6 queries written and tested.
- [✅] All CSV outputs written to output/ folder.
- [✅] .env moved to project root.
- [✅] Git branch created and code pushed successfully.

_Write notes here:_



---

## ❌ What Didn't Go Well / Blockers

| Issue | Root Cause | Resolution |
|-------|------------|------------|
|       |            |            |

---

## 📚 What I Learned Today

1. Building reusable database utility modules (db_utils.py pattern for 90-day usage)
2. Programmatic schema introspection using information_schema queries
3. Writing query functions that return structured data (dicts) for downstream processing
4. Context managers and connection pooling best practices
5. Exporting query results to CSV for validation and documentation

---

## ⏱️ Time Tracking

| Block  | Task | Estimated | Actual | Variance |
|--------|------|-----------|--------|----------|
| A | Branch setup + db_utils.py | 20 min | | |
| B | db_explorer.py schema introspection | 20 min | | |
| C | queries.py (6 analytical queries) | 45 min | | |
| D | CSV export + output review | 15 min | | |
| E | Log + Git push | 20 min | | |
| **Total** | | **2 hr** | | |

---

## 🎯 Carry-Forward to Day 03

- [ ] Q4–Q6 SQL patterns understood for reuse?
- [ ] db_utils module tested with real workloads?
- [ ] Any connection pooling edge cases discovered?
- [ ] CSV output formats match downstream expectations?
- [ ] db_explorer.py template ready for repeating on new databases?

---

## 💡 Feedback Request for Instructor/AI

_Paste the output of q1_top_rental_rate_films() here (first 3 rows should show films with rental_rate > 2.99):_

```
(paste output here)
```

_Paste the output of q2_revenue_by_rating() here (should show 5 rows, one per rating):_

```
(paste output here)
```

_Git log from today (git log --oneline -10 | head -5):_

```
(paste output here)
```

_Any Q4–Q6 SQL challenges or design questions?_

```
(paste notes here)
```

---

## 🔜 PREVIEW: DAY 03

**Topic:** Python libraries installation audit + first use of Pandas with DB data  
**What you'll do:** Load query results directly into Pandas DataFrames, explore `.info()`,
`.describe()`, `.value_counts()`, and write your first DataFrame back to a new PostgreSQL table.

---

*Retrospective: Day 02 | Sprint 01 | EP-01: Environment & Foundations*
