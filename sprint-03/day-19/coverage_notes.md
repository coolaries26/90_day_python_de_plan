etl_resilient.py      49      9    82%   70-71, 75-79, 83-84
## Baseline
- Coverage: 82%
- Uncovered lines: 70-71, 75-79, 83-84

## What those lines do (look them up in etl_resilient.py):
line #  70-71
        df.to_sql(settings.TARGET_TABLE, self.engine, if_exists="replace", index=False)
        logger.info("✅ Loaded {} rows into {}", len(df), settings.TARGET_TABLE)
line # 75-79
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(parents=True, exist_ok=True)   # parents=True handles missing parents
        path = output_dir / settings.OUTPUT_CSV
        df.to_csv(path, index=False)
        logger.info(f"📄 Exported CSV -> {path}") 
line 83-84
    pipeline = ResilientETLPipeline(max_retries=3)
    pipeline.run()

## Tests written to cover them:

    with patch("etl_resilient.get_engine"), \
test_df_to_sql: covers line 70-71
  ->       patch("etl_resilient.pd.read_sql", return_value=sample_df), \
           patch.object(ResilientETLPipeline, "load"), \
test_export_csv_creates_file: covers line 75-79
  ->       patch.object(ResilientETLPipeline, "export_csv"):

        pipeline = ResilientETLPipeline(max_retries=2)
        result = pipeline.run()  # Should succeed on first attempt
        pd.read_sql.assert_called_once()  # type: ignore[attr-defined]
        pipeline.load.assert_called_once()  # type: ignore[attr-defined]
        pipeline.export_csv.assert_called_once()  # type: ignore[attr-defined]
test_load_calls_to_sql: covers line 75-79
test_pipeline_init_max_retries: covers line 83-84
  ->   pipeline = ResilientETLPipeline(max_retries=2)

       with pytest.raises(Exception, match="Persistent DB error"):
            pipeline.run()
   
---

---
---------- coverage: platform win32, python 3.12.4-final-0 -----------
Name                                                             Stmts   Miss  Cover   Missing
----------------------------------------------------------------------------------------------
C:\90_day_python_de_plan\sprint-01\day-02\db_utils.py               96     47    51%   43-44, 62, 81-95, 118-130, 171-176, 183-188, 212-221, 226-230, 238-241
C:\90_day_python_de_plan\sprint-01\day-04\logger.py                 54      6    89%   36, 75, 138-140, 180, 190
C:\90_day_python_de_plan\sprint-02\day-11\config\etl_config.py      11      0   100%
C:\90_day_python_de_plan\sprint-02\day-12\etl_resilient.py          49      2    96%   83-84
C:\90_day_python_de_plan\sprint-02\day-14\etl_protocols.py         109     53    51%   46, 48, 56, 64, 94-99, 103-106, 111-113, 117, 167, 170-173, 176-181, 184, 187, 192-223
conftest.py                                                         45     12    73%   101-104, 117-124, 137-139
test_coverage_gaps.py                                               43      0   100%
test_etl_fixed.py                                                   48      0   100%
----------------------------------------------------------------------------------------------
TOTAL                                                              455    120    74%
---
