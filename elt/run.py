"""
Main ETL orchestration script - Phase 3
Coordinates Extract → Validate → Quality Gates → Transform → Load with incremental processing
"""

import logging
import time
import sys
import argparse
from pathlib import Path
from typing import Tuple, List, Dict

from config import settings
from extract import read_csv_file, compute_file_hash
from transform import transform_user_row
from validate import validate_row, detect_duplicates
from load import (
    init_db, 
    get_session, 
    create_etl_run, 
    update_etl_run,
    check_file_already_processed,
    insert_raw_row_to_staging,
    upsert_user,
    record_bad_row
)

# Configure logging
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_etl(csv_file: str = None, strict_mode: bool = None):
    """
    Main ETL pipeline execution with quality gates and incremental loading
    
    Args:
        csv_file: Override CSV file path
        strict_mode: Override strict mode setting
    """
    
    csv_path = csv_file or settings.CSV_INPUT_PATH
    strict = strict_mode if strict_mode is not None else settings.STRICT_MODE
    
    logger.info("=" * 80)
    logger.info(f"Starting CSV to PostgreSQL ETL Pipeline (Strict Mode: {strict})")
    logger.info("=" * 80)
    
    start_time = time.time()
    run_id = None
    
    try:
        # Step 0: Check file exists
        if not Path(csv_path).exists():
            logger.error(f"CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Step 1: Initialize database
        logger.info("[1/6] Initializing database...")
        init_db()
        
        # Step 2: Compute file hash (for incremental loads)
        logger.info("[2/6] Computing file hash for incremental load detection...")
        file_hash = compute_file_hash(csv_path)
        logger.info(f"File hash: {file_hash}")
        
        # Step 3: Check if file already processed (SKIP logic)
        session = get_session()
        if check_file_already_processed(session, file_hash):
            logger.info("[3/6] File already processed successfully - SKIPPING")
            
            # Create SKIPPED run record
            etl_run = create_etl_run(session, file_hash=file_hash, source_file=csv_path)
            run_id = etl_run.id
            
            duration = int(time.time() - start_time)
            update_etl_run(session, run_id, 0, 0, 0, "SKIPPED", duration_seconds=duration, rows_skipped=0)
            
            logger.info("=" * 80)
            logger.info("ETL Pipeline Complete (SKIPPED - duplicate file)")
            logger.info("=" * 80)
            return True
        
        logger.info("[3/6] File is new - proceeding with full pipeline")
        
        # Step 4: Create ETL run record
        logger.info("[4/6] Creating ETL run record...")
        etl_run = create_etl_run(session, file_hash=file_hash, source_file=csv_path)
        run_id = etl_run.id
        
        # Step 5: Extract, Validate, and collect results
        logger.info("[5/6] Reading and validating CSV rows...")
        
        valid_rows = []
        invalid_rows = []
        rows_total = 0
        
        for extracted_row in read_csv_file(csv_path):
            rows_total += 1
            row_number = extracted_row["row_number"]
            raw_data = extracted_row["data"]
            
            # Validate row
            is_valid, error_msg = validate_row(raw_data)
            
            if is_valid:
                valid_rows.append(raw_data)
            else:
                invalid_rows.append((raw_data, error_msg))
                logger.warning(f"Row {row_number} validation failed: {error_msg}")
            
            if rows_total % 100 == 0:
                logger.info(f"  Scanned {rows_total} rows...")
        
        logger.info(f"Validation complete: {len(valid_rows)} valid, {len(invalid_rows)} invalid")
        
        # Step 6: Quality Gates (PHASE 3 KEY FEATURE)
        logger.info("[6/6] Applying quality gates...")
        
        invalid_ratio = len(invalid_rows) / rows_total if rows_total > 0 else 0
        logger.info(f"Invalid row ratio: {invalid_ratio:.2%} (threshold: {settings.MAX_INVALID_RATIO:.2%})")
        
        # Gate 1: Check invalid ratio
        if invalid_ratio > settings.MAX_INVALID_RATIO:
            error_msg = f"Invalid row ratio {invalid_ratio:.2%} exceeds threshold {settings.MAX_INVALID_RATIO:.2%}"
            logger.error(f"❌ QUALITY GATE FAILED: {error_msg}")
            
            update_etl_run(
                session, run_id,
                rows_processed=rows_total,
                rows_loaded=0,
                rows_failed=len(invalid_rows),
                status="FAILED",
                error_message=error_msg,
                duration_seconds=int(time.time() - start_time)
            )
            
            # Record bad rows
            for raw_row, error_reason in invalid_rows:
                record_bad_row(session, run_id, raw_row, error_reason)
            
            logger.info("=" * 80)
            logger.info("ETL Pipeline Failed - Quality Gate Violation")
            logger.info("=" * 80)
            return False
        
        # Gate 2: Check for duplicate emails
        duplicate_emails, unique_rows = detect_duplicates(valid_rows)
        num_duplicates = len(duplicate_emails)
        
        if num_duplicates > settings.MAX_DUPLICATE_EMAILS:
            error_msg = f"Found {num_duplicates} duplicate emails (threshold: {settings.MAX_DUPLICATE_EMAILS})"
            logger.error(f"❌ QUALITY GATE FAILED: {error_msg}")
            
            if settings.FAIL_ON_DUPLICATE_EMAILS:
                update_etl_run(
                    session, run_id,
                    rows_processed=rows_total,
                    rows_loaded=0,
                    rows_failed=len(invalid_rows),
                    status="FAILED",
                    error_message=error_msg,
                    duration_seconds=int(time.time() - start_time)
                )
                
                for raw_row, error_reason in invalid_rows:
                    record_bad_row(session, run_id, raw_row, error_reason)
                
                logger.info("=" * 80)
                logger.info("ETL Pipeline Failed - Duplicate Email Detection")
                logger.info("=" * 80)
                return False
        
        logger.info(f"✅ All quality gates passed")
        
        # Step 7: Transform and Load (only unique, valid rows)
        logger.info("Loading data into Postgres...")
        
        rows_loaded = 0
        rows_failed_load = 0
        
        for row_num, raw_row in enumerate(unique_rows, start=1):
            try:
                # Transform
                transformed_row = transform_user_row(raw_row)
                
                # Load to staging (for audit)
                insert_raw_row_to_staging(session, transformed_row, run_id)
                
                # Upsert to final table (idempotent on email)
                if upsert_user(session, transformed_row):
                    rows_loaded += 1
                else:
                    rows_failed_load += 1
                    record_bad_row(session, run_id, raw_row, "Failed to upsert to users table")
                
                if row_num % 100 == 0:
                    logger.info(f"  Loaded {row_num}/{len(unique_rows)} rows...")
                    
            except Exception as e:
                logger.error(f"Error loading row {row_num}: {str(e)}")
                record_bad_row(session, run_id, raw_row, f"Load error: {str(e)}")
                rows_failed_load += 1
        
        # Step 8: Update ETL run with final status
        duration_seconds = int(time.time() - start_time)
        update_etl_run(
            session,
            run_id,
            rows_processed=rows_total,
            rows_loaded=rows_loaded,
            rows_failed=len(invalid_rows) + rows_failed_load,
            status="SUCCESS",
            duration_seconds=duration_seconds
        )
        
        logger.info("=" * 80)
        logger.info("✅ ETL Pipeline Complete - SUCCESS")
        logger.info(f"Total Rows Processed:  {rows_total}")
        logger.info(f"Rows Valid:            {len(valid_rows)}")
        logger.info(f"Rows Invalid:          {len(invalid_rows)}")
        logger.info(f"Rows Loaded:           {rows_loaded}")
        logger.info(f"Rows Failed:           {len(invalid_rows) + rows_failed_load}")
        logger.info(f"Duration:              {duration_seconds} seconds")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ FATAL ERROR: {str(e)}", exc_info=True)
        duration_seconds = int(time.time() - start_time)
        
        try:
            if run_id:
                session = get_session()
                update_etl_run(
                    session,
                    run_id,
                    rows_processed=0,
                    rows_loaded=0,
                    rows_failed=0,
                    status="FAILED",
                    error_message=str(e),
                    duration_seconds=duration_seconds
                )
        except:
            pass
        
        return False


def main():
    """CLI entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='CSV to PostgreSQL ETL Pipeline (Phase 3 - Production Mode)'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='CSV file path (overrides env variable)'
    )
    parser.add_argument(
        '--strict',
        type=bool,
        default=settings.STRICT_MODE,
        help='Enable strict mode (fail on quality gate violations)'
    )
    
    args = parser.parse_args()
    
    success = run_etl(csv_file=args.file, strict_mode=args.strict)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
