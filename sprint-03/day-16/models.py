#!/usr/bin/env python3
"""
models.py — Day 16 | SQLAlchemy ORM Models
===========================================
ORM representations of DVD Rental tables + new etl_audit_log table.
These models are used by:
  - Alembic (autogenerate detects schema changes)
  - orm_queries.py (query via Python objects)
  - ETL pipelines (write audit records after each run)

Relationship to actual DB:
  - Film, Customer, Rental, Payment → map to EXISTING tables (reflect schema)
  - AuditLog → NEW table (created by Alembic migration today)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey,
    Integer, Numeric, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ── Declarative Base — all models inherit from this ───────────────────────────
class Base(DeclarativeBase):
    """
    Modern SQLAlchemy 2.0 DeclarativeBase.
    All ORM models must inherit from this class.
    Alembic reads Base.metadata to understand the full schema.
    """
    pass


# ── Film model — fully provided ───────────────────────────────────────────────
class Film(Base):
    """ORM model for the 'film' table — maps existing DVD Rental table."""
    __tablename__ = "film"
    __table_args__ = {"extend_existing": True}

    film_id:          Mapped[int]            = mapped_column(Integer, primary_key=True)
    title:            Mapped[str]            = mapped_column(String(255), nullable=False)
    description:      Mapped[str | None]     = mapped_column(Text)
    release_year:     Mapped[int | None]     = mapped_column(Integer)
    rental_duration:  Mapped[int]            = mapped_column(Integer, nullable=False, default=3)
    rental_rate:      Mapped[Decimal]        = mapped_column(Numeric(4, 2), nullable=False)
    length:           Mapped[int | None]     = mapped_column(Integer)
    replacement_cost: Mapped[Decimal]        = mapped_column(Numeric(5, 2), nullable=False)
    rating:           Mapped[str | None]     = mapped_column(String(10))

    def __repr__(self) -> str:
        return (f"Film(id={self.film_id}, title={self.title!r}, "
                f"rate={self.rental_rate}, rating={self.rating!r})")


# ── Customer model — fully provided ──────────────────────────────────────────
class Customer(Base):
    """ORM model for the 'customer' table."""
    __tablename__ = "customer"
    __table_args__ = {"extend_existing": True}

    customer_id:  Mapped[int]        = mapped_column(Integer, primary_key=True)
    first_name:   Mapped[str]        = mapped_column(String(45), nullable=False)
    last_name:    Mapped[str]        = mapped_column(String(45), nullable=False)
    email:        Mapped[str | None] = mapped_column(String(50))
    active:       Mapped[bool]       = mapped_column(Boolean, nullable=False, default=True)
    create_date:  Mapped[datetime]   = mapped_column(DateTime, nullable=False)

    # Relationship — gives you customer.rentals as a list
    rentals: Mapped[list[Rental]] = relationship("Rental", back_populates="customer")

    @property
    def full_name(self) -> str:
        """Computed property — no column, derived from first + last."""
        return f"{self.first_name} {self.last_name}"

    def __repr__(self) -> str:
        return (f"Customer(id={self.customer_id}, "
                f"name={self.full_name!r}, active={self.active})")


# ── Rental model — WRITE THIS YOURSELF ───────────────────────────────────────
class Rental(Base):
    """
    Q1 — YOUR TASK: ORM model for the 'rental' table.

    HINTS — columns to map:
      rental_id     INTEGER PRIMARY KEY
      rental_date   TIMESTAMP NOT NULL
      inventory_id  INTEGER NOT NULL (FK → inventory.inventory_id)
      customer_id   INTEGER NOT NULL (FK → customer.customer_id)
      return_date   TIMESTAMP NULL        ← nullable — open rentals
      staff_id      INTEGER NOT NULL

    HINTS — relationships:
      customer: Mapped[Customer] = relationship("Customer", back_populates="rentals")

    HINTS — __repr__:
      Rental(id=X, customer_id=Y, rental_date=Z, returned=True/False)
      returned = self.return_date is not None
    """
    __tablename__ = "rental"
    __table_args__ = {"extend_existing": True}
    # YOUR CODE HERE
    rental_id:        Mapped[int]            = mapped_column(Integer, primary_key=True)
    rental_date:      Mapped[datetime]       = mapped_column(DateTime, nullable=False)
#    inventory_id:     Mapped[Integer]        = mapped_column(Integer, ForeignKey("inventory.inventory_id"), nullable=False)
    customer_id:      Mapped[Integer]        = mapped_column(Integer, ForeignKey("customer.customer_id"), nullable=False)
    return_date:      Mapped[datetime | None] = mapped_column(DateTime)
    staff_id:         Mapped[int]            = mapped_column(Integer, nullable=False)

    customer: Mapped[Customer] = relationship("Customer", back_populates="rentals")
    
    @property
    def returned(self) -> bool:
        return self.return_date is not None

    def __repr__(self) -> str:
        return (f"Rental(id={self.rental_id}, customer_id={self.customer_id}, "
                f"rental_date={self.rental_date}, returned={self.returned})")


# ── Payment model — WRITE THIS YOURSELF ──────────────────────────────────────
class Payment(Base):
    """
    Q2 — YOUR TASK: ORM model for the 'payment' table.

    HINTS — columns to map:
      payment_id    INTEGER PRIMARY KEY
      customer_id   INTEGER NOT NULL (FK → customer.customer_id)
      staff_id      INTEGER NOT NULL
      rental_id     INTEGER NULL (FK → rental.rental_id)
      amount        NUMERIC(5,2) NOT NULL
      payment_date  TIMESTAMP NOT NULL

    HINTS — __repr__:
      Payment(id=X, customer_id=Y, amount=Z)
    """
    __tablename__ = "payment"
    __table_args__ = {"extend_existing": True}
    # YOUR CODE HERE
    payment_id:      Mapped[int]            = mapped_column(Integer, primary_key=True)
    customer_id:     Mapped[int]            = mapped_column(Integer, ForeignKey("customer.customer_id"), nullable=False)
    staff_id:        Mapped[int]            = mapped_column(Integer, nullable=False)
    rental_id:       Mapped[int | None]     = mapped_column(Integer, ForeignKey("rental.rental_id"))
    amount:          Mapped[Decimal]        = mapped_column(Numeric(5, 2), nullable=False)
    payment_date:    Mapped[datetime]       = mapped_column(DateTime, nullable=False)

    def __repr__(self) -> str:
        return (f"Payment(id={self.payment_id}, customer_id={self.customer_id}, "
                f"amount={self.amount}, payment_date={self.payment_date})"  )
    
    @property
    def rental(self) -> Rental | None:
        """Optional relationship to Rental — not all payments are for rentals.
        Access via payment.rental if you want the Rental object, or None.
        """
        if self.rental_id is None:
            return None
        # In a real app, you'd set up a proper relationship() here.
        # For this exercise, we'll do a lazy load via session query.
        from sqlalchemy.orm import Session
        with Session.object_session(self) as session:
            return session.get(Rental, self.rental_id)  # type: ignore




# ── AuditLog model — WRITE THIS YOURSELF ─────────────────────────────────────
class AuditLog(Base):
    """
    Q3 — YOUR TASK: ORM model for NEW 'etl_audit_log' table.
    This table does NOT exist yet — Alembic will create it.

    HINTS — columns to map (match the schema in the Background section):
      id             Integer, primary_key=True, autoincrement=True
      pipeline_name  String(100), nullable=False
      source_table   String(100), nullable=False
      target_table   String(100), nullable=False
      rows_loaded    Integer, nullable=False, default=0
      status         String(20), nullable=False
      elapsed_s      Numeric(8,3), nullable=True
      error_message  Text, nullable=True
      run_at         DateTime, nullable=False, server_default=func.now()
                     ↑ server_default means PostgreSQL fills it in, not Python

    HINTS — @classmethod from_result():
      Takes an ETLResult object, returns an AuditLog instance.
      This is the bridge between your Day 14 ETLResult and the DB table.

    HINTS — __repr__:
      AuditLog(id=X, pipeline=Y, status=Z, rows=N, run_at=T)
    """
    __tablename__ = "etl_audit_log"
    __table_args__ = {"extend_existing": True}
    # YOUR CODE HERE
    id:              Mapped[int]            = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_name:   Mapped[str]            = mapped_column(String(100), nullable=False)
    source_table:    Mapped[str]            = mapped_column(String(100), nullable=False)
    target_table:    Mapped[str]            = mapped_column(String(100), nullable=False)
    rows_loaded:     Mapped[int]            = mapped_column(Integer, nullable=False, default=0)
    status:          Mapped[str]            = mapped_column(String(20), nullable=False)
    elapsed_s:       Mapped[Decimal | None] = mapped_column(Numeric(8, 3), nullable=True)
    error_message:   Mapped[str | None]     = mapped_column(Text, nullable=True)
    run_at:          Mapped[datetime]       = mapped_column(DateTime, nullable=False, server_default=func.now())  

    def __repr__(self) -> str:
        return (f"AuditLog(id={self.id}, pipeline={self.pipeline_name!r}, "
                f"status={self.status!r}, rows={self.rows_loaded}, run_at={self.run_at})")
    @property
    def successful(self) -> bool:
        return self.status == "SUCCESS"

    @classmethod
    def from_result(cls, result: object) -> AuditLog:
        """
        Q3b — Create AuditLog from ETLResult.

        HINTS:
          - Import ETLResult at top of function to avoid circular import:
            from etl_protocols import ETLResult
          - Access: result.pipeline_name, result.source_table,
                    result.target_table, result.rows_loaded,
                    result.status, result.elapsed_seconds, result.error_message
        """
        # YOUR CODE HERE
        try:
            from etl_protocols import ETLResult
        except ImportError:
            raise NotImplementedError("Implement AuditLog.from_result()")
        return cls(
            pipeline_name=result.pipeline_name,
            source_table=result.source_table,
            target_table=result.target_table,
            rows_loaded=result.rows_loaded,
            status=result.status,
            elapsed_s=Decimal(result.elapsed_seconds) if result.elapsed_seconds is not None else None,
            error_message=result.error_message
        )    
