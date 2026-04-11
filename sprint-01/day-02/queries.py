# queries.py — Day 02 Analytical Queries on DVD Rental DB
# =========================================================
# 6 queries progressing from simple to multi-table joins.
# Each function returns a list of dicts and writes a CSV.
# 
# Run: python queries.py
# one more time with feeling
import csv
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from db_utils import execute_query, close_pool, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def write_csv(filename: str, rows: list[dict]) -> Path:
    """Write query results to a CSV file."""
    path = OUTPUT_DIR / filename
    if not rows:
        print(f"  ⚠️  No rows returned for {filename}")
        return path
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return path


# ── Q1: Simple SELECT with filter and ORDER BY ────────────────────────────────
def q1_top_rental_rate_films() -> list[dict]:
    """
    Q1: Top 10 most expensive films to rent, showing title,
        rating, rental_rate, and rental_duration.
    Concepts: SELECT, WHERE, ORDER BY, LIMIT
    """
    sql = """
        SELECT
            film_id,
            title,
            rating,
            rental_rate,
            rental_duration,
            length          AS duration_minutes
        FROM film
        WHERE rental_rate > 2.99
        ORDER BY rental_rate DESC, title ASC
        LIMIT 10;
    """
    return execute_query(sql, as_dict=True)


# ── Q2: Aggregation + GROUP BY ─────────────────────────────────────────────────
def q2_revenue_by_rating() -> list[dict]:
    """
    Q2: Total payment revenue grouped by film rating.
    Shows how different rating categories perform commercially.
    Concepts: JOIN, GROUP BY, SUM, ROUND, ORDER BY aggregate
    """
    sql = """
        SELECT
            f.rating,
            COUNT(DISTINCT f.film_id)           AS film_count,
            COUNT(r.rental_id)                  AS total_rentals,
            ROUND(SUM(p.amount)::numeric, 2)    AS total_revenue,
            ROUND(AVG(p.amount)::numeric, 2)    AS avg_payment
        FROM film f
        JOIN inventory i   ON f.film_id   = i.film_id
        JOIN rental r      ON i.inventory_id = r.inventory_id
        JOIN payment p     ON r.rental_id    = p.rental_id
        GROUP BY f.rating
        ORDER BY total_revenue DESC;
    """
    return execute_query(sql, as_dict=True)


# ── Q3: Multi-table JOIN with date functions ──────────────────────────────────
def q3_monthly_rental_trend() -> list[dict]:
    """
    Q3: Monthly rental counts and revenue for the full dataset.
    Reveals seasonality — useful for forecasting pipelines.
    Concepts: date_trunc, multi-join, time series aggregation
    """
    sql = """
        SELECT
            DATE_TRUNC('month', r.rental_date)::date   AS rental_month,
            COUNT(r.rental_id)                         AS rentals,
            COUNT(DISTINCT r.customer_id)              AS unique_customers,
            ROUND(SUM(p.amount)::numeric, 2)           AS monthly_revenue
        FROM rental r
        JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY DATE_TRUNC('month', r.rental_date)
        ORDER BY rental_month;
    """
    return execute_query(sql, as_dict=True)


# ── Q4: HINT ONLY — write this yourself ──────────────────────────────────────
def q4_top_10_customers_by_spend() -> list[dict]:
    """
    Q4: Top 10 customers by total spend.
    Expected columns: customer_id, full_name, email, total_spend,
                      rental_count, avg_spend_per_rental
    HINTS:
      - JOIN: customer → rental → payment
      - Concatenate first_name + ' ' + last_name for full_name
      - Use SUM(amount) for total_spend
      - Use COUNT(rental_id) for rental_count
      - ROUND to 2 decimal places
      - ORDER BY total_spend DESC LIMIT 10
    """
    sql = """
        SELECT
            c.customer_id, 
            CONCAT(c.first_name, ' ', c.last_name) AS full_name, 
            c.email, 
            SUM(p.amount) AS total_spend,
            COUNT(r.rental_id) AS rental_count,
            ROUND(AVG(p.amount), 2) AS avg_spend_per_rental
        FROM customer c
        JOIN rental r ON c.customer_id = r.customer_id
        JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY c.customer_id, full_name, c.email
        ORDER BY total_spend DESC
        LIMIT 10;
    """
    return execute_query(sql, as_dict=True)


# ── Q5: HINT ONLY — write this yourself ──────────────────────────────────────
def q5_category_popularity() -> list[dict]:
    """
    Q5: Film category rental popularity + revenue.
    Expected columns: category_name, total_rentals, total_revenue,
                      avg_rental_rate, film_count

    HINTS:
      - Tables: category → film_category → film → inventory → rental → payment
      - GROUP BY category.name
      - ORDER BY total_rentals DESC
    """
    sql = """
        -- YOUR SQL HERE
        SELECT c.name AS category_name, 
        COUNT(r.rental_id) AS total_rentals, 
        SUM(p.amount) AS total_revenue, 
        ROUND(AVG(f.rental_rate)::numeric, 4) AS avg_rental_rate, 
        COUNT(DISTINCT f.film_id) AS film_count
        FROM film_category fc
        JOIN category c ON fc.category_id = c.category_id
        JOIN film f ON fc.film_id = f.film_id
        JOIN inventory i ON f.film_id = i.film_id
        JOIN rental r ON i.inventory_id = r.inventory_id
        JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY c.name
        ORDER BY total_rentals DESC
        LIMIT 10;
    """
    return execute_query(sql, as_dict=True)


# ── Q6: HINT ONLY — write this yourself ──────────────────────────────────────
def q6_overdue_rentals() -> list[dict]:
    """
    Q6: Rentals that were returned late (return_date > rental_date + rental_duration days).
    Expected columns: rental_id, customer_name, film_title, rental_date,
                      due_date, return_date, days_overdue

    HINTS:
      - A rental's due_date = rental_date + INTERVAL '(rental_duration) days'
      - Filter: return_date > due_date
      - EXTRACT(EPOCH FROM (return_date - due_date))/86400 gives days_overdue as float
      - Include only rentals where return_date IS NOT NULL
      - ORDER BY days_overdue DESC LIMIT 20
    """
    sql = """
        -- YOUR SQL HERE
        SELECT 
            r.rental_id, 
            CONCAT(c.first_name, ' ', c.last_name) AS customer_name, 
            f.title AS film_title, 
            r.rental_date,
            r.rental_date + (f.rental_duration || ' days')::interval AS due_date, 
            r.return_date, 
            ROUND(EXTRACT(EPOCH FROM (r.return_date - (r.rental_date + (f.rental_duration || ' days')::interval)))/86400, 2) AS days_overdue
        FROM rental r
        JOIN customer c ON c.customer_id=r.customer_id
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.return_date IS NOT NULL
        ORDER BY days_overdue DESC
        LIMIT 20;
    """
    return execute_query(sql, as_dict=True)


# ── Runner ─────────────────────────────────────────────────────────────────────
def run_query(label: str, fn) -> list[dict]:
    """Execute a query function, print preview, write CSV."""
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"{'─'*50}")
    try:
        rows = fn()
        if rows and "reminder" in rows[0]:
            print(f"  ⏳ Not implemented yet — write the SQL in queries.py")
            return []
        print(f"  Rows returned: {len(rows)}")
        if rows:
            # Print first 3 rows as preview
            headers = list(rows[0].keys())
            print(f"  Columns: {', '.join(headers)}")
            for i, row in enumerate(rows[:3]):
                print(f"  [{i+1}] {dict(row)}")
        return rows
    except Exception as exc:
        print(f"  ❌ Error: {exc}")
        return []


def main():
    print("\n🔍 DVD Rental — Analytical Queries")
    print(f"   Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 52)

    queries = [
        ("Q1: Top 10 Films by Rental Rate",   q1_top_rental_rate_films,    "q1_top_rental_rate_films.csv"),
        ("Q2: Revenue by Film Rating",         q2_revenue_by_rating,         "q2_revenue_by_rating.csv"),
        ("Q3: Monthly Rental Trend",           q3_monthly_rental_trend,      "q3_monthly_rental_trend.csv"),
        ("Q4: Top 10 Customers by Spend",      q4_top_10_customers_by_spend, "q4_top_customers.csv"),
        ("Q5: Category Popularity + Revenue",  q5_category_popularity,       "q5_category_popularity.csv"),
        ("Q6: Overdue Rentals",                q6_overdue_rentals,           "q6_overdue_rentals.csv"),
    ]

    for label, fn, csv_name in queries:
        rows = run_query(label, fn)
        if rows:
            out = write_csv(csv_name, rows)
            print(f"  📄 CSV → {out.name}")

    print("\n" + "=" * 52)
    print("  All queries complete. Check output/ folder.")
    print("=" * 52 + "\n")

    close_pool()
    dispose_engine()


if __name__ == "__main__":
    main()
