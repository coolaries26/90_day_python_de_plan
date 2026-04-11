🔍 Day 02 Code Review — Brutal & Honest
Overall: 6/6 queries working. Solid execution. Now the details.

✅ What's Good
Q4 — Correct. Eleanor Hunt at $211.55 / 45 rentals is the canonical top spender in this dataset. Your join chain and column aliasing are right.
Q5 — Correct. Sports → Animation → Action is the expected top-3. However, one issue:
avg_rental_rate: Decimal('4.5256151711378353')   ← 16 decimal places
You didn't ROUND() this column. Q2 revenue was rounded to 4dp, Q5 rate is unrounded. Inconsistency in a pipeline output is a data quality bug. Fix: ROUND(AVG(f.rental_rate)::numeric, 4).
Q6 — Correct logic, but same issue:
days_overdue: Decimal('6.2486111111111111')   ← 16 decimal places
ROUND(..., 2) is missing. In a real pipeline this bloats CSV file size and causes downstream type mismatches.
Q3 — Only 4 months returned. That's correct for this dataset (Jun–Aug 2005 + Feb 2006) — but a senior engineer would add a comment in the code noting this is expected, not a missing data bug. Add it.

⚠️ One Structural Issue — avg_rental_rate in Q5
avg_rental_rate belongs to film, not to rental transactions. Mixing it with total_rentals and total_revenue (which are transaction-level) in the same row is semantically misleading. In a real data model these would be in separate fact/dimension layers. Keep it for now, but remember this distinction in Sprint 07 when we build proper dimensional models.

Score: 5.5/6 — deduct 0.5 for the two unrounded decimals. Fix before Day 03 branch is merged.
bash# Quick fix — rerun after fixing the two ROUND() calls
python queries.py
git add sprint-01/day-02/
git commit -m "[DAY-002][FIX] ROUND avg_rental_rate and days_overdue to 4dp and 2dp"
git push
Now — Day 03.Created a file, ran a command, read a fileCreated a file, ran a command, read a file

📋 Day 02 Feedback Summary
QueryResultVerdictQ110 rows, correct films✅Q2PG-13 top revenue — correct✅Q34 months — correct for this dataset✅Q4Eleanor Hunt $211.55 — perfect✅Q5Sports first — correct, but avg_rental_rate unrounded⚠️ FixQ6Correct logic, but days_overdue unrounded⚠️ Fix
Fix those two ROUND() calls first — then Day 03 begins.