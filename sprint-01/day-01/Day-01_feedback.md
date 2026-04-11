 feedback on Day 01: 36/36 — clean. Two things to note before Day 02:

You're on Python 3.12.4 (not 3.11 as planned) — no issue, 3.12 is fine and actually faster. Requirements stay the same.
.env landed inside sprint-01/day-01/ not at project root. That worked due to our path-search fix, but move it to project root today — every future script will expect it there.