| Level  | Task      | Description |
|--------|-----------|-------------|  
|INFO  | Step 1          | Connecting to database and counting rows                                |
|INFO  | Table inventory | table=film rows=1,000                                                   |
|DEBUG | Table details   | table=film dtype_check=pending                                          |
|INFO  | Table inventory | table=customer rows=599                                                 |
|DEBUG | Table details   | table=customer dtype_check=pending                                      |
|INFO  | Table inventory | table=rental rows=16,044                                                |
|DEBUG | Table details   | table=rental dtype_check=pending                                        |
|INFO  | Table inventory | table=payment rows=14,596                                               |
|DEBUG | Table details   | table=payment dtype_check=pending                                       |
|INFO  | Table inventory | table=inventory rows=4,581                                              |
|DEBUG | Table details   | table=inventory dtype_check=pending                                     |
|INFO  | Step 2          | Checking data quality thresholds                                        |
|INFO  | Data quality    | null return_date within threshold, pct=1.1%                             |
|INFO  | Step 3          | Testing error recovery                                                  |
|WARNING | step_3        |not implemented yet: Implement step_3 error handling — see hints above   |
|INFO  | Step 4          | Writing pipeline summary                                                |
|INFO  | Pipeline summary | {'pipeline': 'demo_pipeline', 'tables_checked': 5, 'status': 'SUCCESS'}|
|INFO  | PIPELINE END: demo_pipeline | SUCCESS|
||||


