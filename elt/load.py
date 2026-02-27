"""
Load phase: Insert/upsert data into PostgreSQL with incremental load support
"""

import logging
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Dict, Tuple

from models import Base, StagingUsers, ETLRun, ETLBadRows, Users
from config import settings

logger = logging.getLogger(__name__)


def create_db_engine():
    """Create SQLAlchemy engine"""
    return create_engine(settings.DATABASE_URL, echo=False)


def init_db():
    """Create all tables if they don't exist"""
    engine = create_db_engine()
    logger.info("Creating tables if not exist...")
    Base.metadata.create_all(engine)
    logger.info("Database initialization complete")


def get_session() -> Session:
    """Get database session"""
    engine = create_db_engine()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def check_file_already_processed(session: Session, file_hash: str) -> bool:
    """
    Check if file has already been successfully processed (watermark check)
    
    Args:
        session: Database session
        file_hash: SHA256 hash of file
        
    Returns:
        True if already processed successfully, False otherwise
    """
    existing_run = session.query(ETLRun).filter(
        ETLRun.file_hash == file_hash,
        ETLRun.status == 'SUCCESS'
    ).first()
    
    return existing_run is not None


def create_etl_run(session: Session, file_hash: str = None, source_file: str = None) -> ETLRun:
    """Create a new ETL run record"""
    run = ETLRun(file_hash=file_hash, source_file=source_file)
    session.add(run)
    session.commit()
    session.refresh(run)
    logger.info(f"Created ETL run: {run.id}")
    return run


def update_etl_run(session: Session, run_id: int, rows_processed: int, rows_loaded: int, rows_failed: int, status: str, error_message: str = None, duration_seconds: int = None, rows_skipped: int = 0):
    """Update ETL run with completion stats"""
    run = session.query(ETLRun).filter(ETLRun.id == run_id).first()
    if run:
        run.rows_processed = rows_processed
        run.rows_loaded = rows_loaded
        run.rows_failed = rows_failed
        run.rows_skipped = rows_skipped
        run.status = status
        run.error_message = error_message
        run.duration_seconds = duration_seconds
        session.commit()
        logger.info(f"Updated ETL run {run_id}: {status} (Processed={rows_processed}, Loaded={rows_loaded}, Failed={rows_failed})")


def insert_raw_row_to_staging(session: Session, raw_row: Dict[str, str], run_id: int) -> int:
    """
    Insert raw CSV row to staging table
    
    Args:
        session: Database session
        raw_row: Raw CSV row as dictionary
        run_id: ETL run ID for tracking
        
    Returns:
        Staging row ID
    """
    staging_record = StagingUsers(
        raw_data=json.dumps(raw_row),
        etl_run_id=run_id
    )
    session.add(staging_record)
    session.commit()
    
    return staging_record.id


def upsert_user(session: Session, user_data: Dict[str, str]) -> bool:
    """
    Upsert user into final users table (insert or update on email conflict)
    
    Args:
        session: Database session
        user_data: Transformed user data
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use raw SQL for PostgreSQL UPSERT
        upsert_sql = text("""
            INSERT INTO users (id, name, email, country, created_at, inserted_at, updated_at)
            VALUES (:id, :name, :email, :country, :created_at, NOW(), NOW())
            ON CONFLICT (email) DO UPDATE SET 
                name = EXCLUDED.name,
                country = EXCLUDED.country,
                updated_at = NOW()
            WHERE users.email = EXCLUDED.email
        """)
        
        session.execute(upsert_sql, {
            'id': user_data.get('id'),
            'name': user_data.get('name'),
            'email': user_data.get('email'),
            'country': user_data.get('country'),
            'created_at': user_data.get('created_at')
        })
        session.commit()
        return True
    except Exception as e:
        logger.error(f"Error upserting user {user_data.get('email')}: {str(e)}")
        return False


def record_bad_row(session: Session, run_id: int, raw_row: Dict[str, str], error_reason: str):
    """Record a row that failed validation"""
    bad_row = ETLBadRows(
        etl_run_id=run_id,
        raw_row=json.dumps(raw_row),
        error_reason=error_reason
    )
    session.add(bad_row)
    session.commit()
