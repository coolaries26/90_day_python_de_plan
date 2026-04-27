# Test Failure Root Cause Analysis — Day 18

## test_successful_etl — FileNotFoundError

### What failed:
export_csv() called Path("sprint-02/day-12/output").mkdir()
Running from sprint-02/day-13/ → relative path resolved incorrectly

### Root causes:
1. export_csv() not mocked — real filesystem called from unit test
2. Hardcoded relative path in etl_resilient.py (fixed Day 14)

### Fix:
- Mock export_csv with @patch.object(ResilientETLPipeline, "export_csv")
- OR use tmp_path fixture and patch output_dir

## test_retry_on_failure — StopIteration

### What failed:
mock_read_sql.side_effect = [Exception("DB error"), MagicMock()]
Attempt 1: read_sql raises Exception ✅
Attempt 2: read_sql returns MagicMock ✅ — but export_csv fails
Attempt 3: retry fires → read_sql has no more items → StopIteration

### Root causes:
1. export_csv not mocked — caused unexpected extra retry
2. side_effect list had only 2 items but 3 attempts were made

### Fix:
- Mock export_csv → only 2 read_sql calls happen
- side_effect = [Exception("DB error"), MagicMock()] is then correct

## Lesson:
In unit tests for ETL pipelines:
ALWAYS mock: get_engine, pd.read_sql, df.to_sql, export_csv, time.sleep
NEVER mock: the retry logic itself (that's what you're testing)