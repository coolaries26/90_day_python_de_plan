"""
models_compat.py — SQLAlchemy 1.4 compatible AuditLog
Used by Airflow DAGs running in WSL2 airflow-venv (SQLAlchemy 1.4)
Windows venv uses models.py (SQLAlchemy 2.0)
"""
from sqlalchemy import Column, Integer, String, Numeric, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class AuditLog(Base):
    __tablename__ = "etl_audit_log"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(100), nullable=False)
    source_table  = Column(String(100), nullable=False)
    target_table  = Column(String(100), nullable=False)
    rows_loaded   = Column(Integer, nullable=False, default=0)
    status        = Column(String(20), nullable=False)
    elapsed_s     = Column(Numeric(8, 3), nullable=True)
    error_message = Column(Text, nullable=True)
    run_at        = Column(DateTime, nullable=False, server_default=func.now())
    value_tier    = Column(String(20), nullable=True)

    def __repr__(self) -> str:
        return (f"AuditLog(id={self.id}, pipeline={self.pipeline_name!r}, "
                f"status={self.status!r}, rows={self.rows_loaded})")