
"""
`chart_rental_timeline()` in a new file `sprint-05/day-35/test_charts.py`.

**Requirements:**
- Source: query the `rental` table directly (not analytics tables)
- Chart type: line chart showing rental count per week
- X axis: week start date (use `DATE_TRUNC('week', rental_date)`)
- Y axis: rental count
- Add a horizontal line for the average weekly rentals
- Save as PNG to `sprint-05/day-35/output/rental_timeline.png`
- Use `matplotlib.use("Agg")` — no display
**SQL hint:**
```sql
SELECT DATE_TRUNC('week', rental_date)::date AS week_start,
       COUNT(*) AS rental_count
FROM rental
GROUP BY DATE_TRUNC('week', rental_date)
ORDER BY week_start
```
"""

from __future__ import annotations
import os
import sys
#from turtle import st

import matplotlib.pyplot as plt
#import matplotlib.use("Agg")
import matplotlib.patches as mpatches
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
_root = Path(__file__).resolve().parent.parent.parent
insert_path = _root / "sprint-01" / "day-02"
sys.path.insert(0, str(_root / "sprint-01" / "day-02"))
sys.path.insert(1, str(_root / "sprint-05" / "day-35"))

sys.path.append(str(insert_path))

from db_utils import get_engine, dispose_engine, execute_query, execute_scalar

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def chart_rental_timeline() -> Path:
    # Connect to the PostgreSQL database
    def _engine():
        """Get or create SQLAlchemy engine. Reuses singleton from db_utils."""
        return get_engine()


#    @st.cache_data(ttl=300)   # cache 5 minutes — refresh every 5 min
    def load_rental() -> pd.DataFrame:
        """Source: query the `rental` table directly (not analytics tables)"""
        return pd.read_sql(
            "SELECT DATE_TRUNC('week', rental_date)::date AS week_start, " \
            "COUNT(*) AS rental_count " \
            "FROM rental " \
            "GROUP BY DATE_TRUNC('week', rental_date) " \
            "ORDER BY week_start",
            _engine()
        )

#
#    @st.cache_data(ttl=300)
#    def load_films() -> pd.DataFrame:
#        """Load rental table."""
#        return pd.read_sql(
#            """
#                    SELECT DATE_TRUNC('week', rental_date)::date AS week_start,
#                   COUNT(*) AS rental_count
#            FROM rental
#            GROUP BY DATE_TRUNC('week', rental_date)
#            ORDER BY week_start
#            """,
#            _engine()
#        )

    dispose_engine()  # Ensure we clean up the engine after use
# Convert results to a DataFrame
    df = pd.DataFrame(load_rental(), columns=['week_start', 'rental_count'])

# Calculate the average weekly rentals
    average_weekly_rentals = df['rental_count'].mean()

    # Create a line chart
    plt.figure(figsize=(10, 6))
    plt.plot(df['week_start'], df['rental_count'], marker='o', label='Weekly Rentals')
    plt.axhline(y=average_weekly_rentals, color='r', linestyle='--', label=f'Average Weekly Rentals ({average_weekly_rentals:.2f})')
    plt.title('Weekly Rental Timeline')
    plt.xlabel('Week Start Date')
    plt.ylabel('Rental Count')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    # Save the chart as a PNG file
    
    out = OUTPUT_DIR / "c6_rental_timeline.png"
    plt.savefig(out)
    #    plt.savefig('sprint-05/day-35/output/rental_timeline.png')

    return out

if __name__ == "__main__":
    chart_rental_timeline() 