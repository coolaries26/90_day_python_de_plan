#1
there is a prolblem in runing duplicate check.The error occurs because one of DataFrame columns contains unhashable types (likely lists) that can't be checked for duplicates. The special_features column in the film table probably stores lists, which causes the duplicated() method to fail.

Solution
Modify your duplicate check to handle unhashable types gracefully:
```# ── Duplicate check (line 75) ──
try:
    dupes = df.duplicated().sum()
except TypeError:
    # Handle columns with unhashable types (e.g., lists)
    hashable_cols = [col for col in df.columns 
                     if df[col].dtype == 'object' and not df[col].apply(lambda x: isinstance(x, list)).any()]
    if hashable_cols:
        dupes = df[hashable_cols].duplicated().sum()
    else:
        dupes = 0
    print(f"  ⚠️  Skipped unhashable columns (lists/dicts); checked {len(hashable_cols)} hashable cols")
```
Or, more simply, convert list columns to strings first:
```
# Before profiling in main():
for name, df in dfs.items():
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(str)
    summary = profile_dataframe(name, df)
    summaries.append(summary)
```
The special_features column likely contains PostgreSQL arrays that are loaded as Python lists. Converting them to strings allows pandas to check for duplicates without errors.

#2
    Write a DataFrame to PostgreSQL.
    if_exists='replace' → drop and recreate table (safe for analytics tables)
    if_exists='append'  → add rows (for incremental loads)
create GRANT CREATE ON SCHEMA public TO appuser; was not there as it was revoked intentionally during sprint-01/day-01/setup_PostgreSQL.sh for security reasons.

#3
create dataframe for Task 3 data masking and dp was missing initially but later it was fixed.
