# Run these and paste the output:

# 1. T3 + analytics tables
python sprint-01/day-03/data_explorer.py

# 2. Verify tables in DB
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import get_engine, dispose_engine
import pandas as pd
engine = get_engine()
df = pd.read_sql(\"SELECT tablename FROM pg_tables WHERE tablename LIKE 'analytics_%' ORDER BY tablename\", engine)
print(df.to_string(index=False))
dispose_engine()
"

# 3. Git log confirmation
git log --oneline -5