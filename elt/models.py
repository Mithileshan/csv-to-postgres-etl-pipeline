"""
SQLAlchemy models for ETL pipeline
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class StagingUsers(Base):
    """Raw staging table for user data before validation"""
    __tablename__ = "staging_users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_data = Column(Text, nullable=False)  # Raw CSV row
    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    etl_run_id = Column(Integer, nullable=True)


class Users(Base):
    """Clean users table after validation and transformation"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    country = Column(String(100), nullable=True)
    created_at = Column(String(50), nullable=True)
    inserted_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)


class ETLRun(Base):
    """Track every ETL run for auditing and debugging"""
    __tablename__ = "etl_runs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_date = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    source_file = Column(String(255), nullable=True)  # Filename/path
    file_hash = Column(String(64), unique=True, nullable=True)  # SHA256 hash of CSV
    rows_processed = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    rows_failed = Column(Integer, default=0)
    rows_skipped = Column(Integer, default=0)
    status = Column(String(50), default="IN_PROGRESS")  # IN_PROGRESS, SUCCESS, FAILED, SKIPPED
    error_message = Column(Text, nullable=True)
    duration_seconds = Column(Integer, nullable=True)


class ETLBadRows(Base):
    """Store rows that failed validation for analysis"""
    __tablename__ = "etl_bad_rows"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    etl_run_id = Column(Integer, nullable=False)
    raw_row = Column(Text, nullable=False)
    error_reason = Column(String(500), nullable=False)
    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
