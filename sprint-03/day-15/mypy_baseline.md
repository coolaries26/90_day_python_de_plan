
---
# mypy Baseline Report — Day 15

## etl_protocols.py

### ERROR: sprint-02\day-14\etl_protocols.py:214: error: Argument "export_csv" to "complete" of "ETLResult" has incompatible type "str"; expected "Path"  [arg-type]

```bash
$ mypy sprint-02/day-14/etl_protocols.py --ignore-missing-imports
sprint-02\day-14\etl_protocols.py:214: error: Argument "export_csv" to "complete" of "ETLResult" has incompatible type "str"; expected "Path"  [arg-type]
Found 1 error in 1 file (checked 1 source file)
```

## oop_etl.py

### ERROR: print-02\day-14\etl_protocols.py:214: error: Argument "export_csv" to "complete" of "ETLResult" has incompatible type "str"; expected "Path"  [arg-type]
 
 ```bash
$ mypy sprint-02/day-14/oop_etl.py --ignore-missing-imports
sprint-02\day-14\etl_protocols.py:214: error: Argument "export_csv" to "complete" of "ETLResult" has incompatible type "str"; expected "Path"  [arg-type]
Found 1 error in 1 file (checked 1 source file)
```

# Fix all mypy errors in `etl_protocols.py`
changed line 214 
### Before:
` result.complete(rows_extracted=599, rows_loaded=599, export_csv="c:\\output_dir\\analytics_customer_oop.csv", attempts=1)` 
### After: 
` result.complete(rows_extracted=599, rows_loaded=599, export_csv=Path("c:\\output_dir\\analytics_customer_oop.csv"), attempts=1)`