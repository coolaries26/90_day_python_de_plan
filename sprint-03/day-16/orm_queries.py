#!/usr/bin/env python3
"""
orm_queries.py — Day 16 | ORM Queries + Audit Log Writer
=========================================================
Demonstrates:
  - SQLAlchemy ORM Session for queries
  - ORM query vs raw SQL — same result, different syntax
  - Writing ETLResult to etl_audit_log via AuditLog.from_result()

Run: python orm_queries.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02")) # for db_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # for logger
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-02" / "day-14")) # for etl_protocols
sys.path.insert(0, str(Path(__file__).resolve().parent)) # for models

from sqlalchemy.orm import Session
from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
from models import Film, Customer, Rental, AuditLog
from etl_protocols import ETLResult


logger = get_pipeline_logger("orm_queries")
engine = get_engine()


# ── Q1: ORM queries — fully provided ─────────────────────────────────────────
def q1_top_films_by_rate(limit: int = 5) -> list[Film]:
    """
    ORM equivalent of:
      SELECT * FROM film WHERE rental_rate > 2.99
      ORDER BY rental_rate DESC LIMIT 5

    Key difference from raw SQL:
      - Returns Film objects, not tuples
      - film.title, film.rental_rate — named attributes, not index positions
      - Session manages connection lifecycle — no manual pool.putconn()
    """
    with Session(engine) as session:
        films = (
            session.query(Film)
            .filter(Film.rental_rate > 2.99)
            .order_by(Film.rental_rate.desc(), Film.title.asc())
            .limit(limit)
            .all()
        )
    # Session closes here — connection returned to pool automatically
    return films


def q2_active_customer_count() -> int:
    """ORM equivalent of: SELECT COUNT(*) FROM customer WHERE active = TRUE"""
    with Session(engine) as session:
        count: int = (
            session.query(Customer)
            .filter(Customer.active == 1)  # noqa: E712 — SQLAlchemy uses == for boolean comparisons
            .count()
        )
    return count


# ── Q2: Write this yourself ───────────────────────────────────────────────────
def q3_customer_rental_count(limit: int = 10) -> list[tuple[str, int]]:
    """
    Q2 — YOUR TASK:
    ORM query — top customers by rental count.
    Expected return: list of (full_name, rental_count) tuples, sorted desc.

    HINTS:
      - Use Session(engine) context manager
      - Query Customer, join Rental via relationship or explicit join
      - Use func.count(Rental.rental_id) for counting
      - Group by Customer.customer_id, Customer.first_name, Customer.last_name
      - order_by count descending, limit to `limit` param
      - For full_name in Python (not SQL): use the @property
        OR use (Customer.first_name + " " + Customer.last_name).label("full_name")

    Expected top result:
      ('Eleanor Hunt', 45) or similar high-rental customer

    IMPORTANT: import func from sqlalchemy at top of this function:
      from sqlalchemy import func
    """
    # YOUR CODE HERE
    try:
        with Session(engine) as session:
            from sqlalchemy import func
    except Exception as e:
        logger.error(f"Error in q3_customer_rental_count: {e}")
        raise NotImplementedError("Q3 not implemented yet") from e  

    results = (
            session.query(
                (Customer.first_name + " " + Customer.last_name).label("full_name"),
                func.count(Rental.rental_id).label("rental_count")
            )
            .join(Rental, Rental.customer_id == Customer.customer_id)
            .group_by(Customer.customer_id, Customer.first_name, Customer.last_name)
            .order_by(func.count(Rental.rental_id).desc())
            .limit(limit)
            .all()
        )
    return results
    

# ── Audit log writer ──────────────────────────────────────────────────────────
def write_audit_log(result: ETLResult) -> None:
    """
    Write an ETLResult to etl_audit_log table via ORM.
    Called at end of every pipeline run from Day 16 onward.
    """
    audit = AuditLog.from_result(result)
    with Session(engine) as session:
        session.add(audit)
        session.commit()
        logger.info(f"Audit log written | pipeline={result.pipeline_name} "
                    f"status={result.status} rows={result.rows_loaded}")


def read_audit_log(limit: int = 5) -> list[AuditLog]:
    """Read recent audit log entries — verify writes are working."""
    with Session(engine) as session:
        entries = (
            session.query(AuditLog)
            .order_by(AuditLog.run_at.desc())
            .limit(limit)
            .all()
        )
        # Expunge — detach from session so objects can be used after session closes
        for entry in entries:
            session.expunge(entry)
    return entries


def main() -> None:
    logger.info("=" * 52)
    logger.info("ORM Queries Demo — Day 16")
    logger.info("=" * 52)

    # Q1 — top films
    logger.info("\n── Q1: Top 5 Films by Rental Rate ──────────")
    films = q1_top_films_by_rate(5)
    for film in films:
        logger.info(f"  {film!r}")

    # Q2 — active customers
    logger.info("\n── Q2: Active Customer Count ────────────────")
    count = q2_active_customer_count()
    logger.info(f"  Active customers: {count}")

    # Q3 — rental counts (your implementation)
    logger.info("\n── Q3: Top 10 Customers by Rental Count ────")
    try:
        rental_counts = q3_customer_rental_count(10)
        for name, cnt in rental_counts:
            logger.info(f"  {name:<25} {cnt} rentals")
    except NotImplementedError as e:
        logger.warning(f"  {e}")

    # Audit log demo — simulate a pipeline result
    logger.info("\n── Audit Log Write + Read ───────────────────")
    fake_result = ETLResult(
        pipeline_name="DemoETL",
        source_table="customer",
        target_table="analytics_customer_oop",
    )
    fake_result.complete(rows_extracted=599, rows_loaded=599, export_csv=Path("fake_export.csv"), attempts=1)

    write_audit_log(fake_result)

    entries = read_audit_log(3)
    for entry in entries:
        logger.info(f"  {entry!r}")

    dispose_engine()
    logger.info("Done.")


if __name__ == "__main__":
    main()