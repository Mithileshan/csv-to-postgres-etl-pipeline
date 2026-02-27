"""
Extract phase: Read data from CSV file with file hashing
"""

import csv
import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Generator, Tuple

logger = logging.getLogger(__name__)


def compute_file_hash(file_path: str) -> str:
    """
    Compute SHA256 hash of file for incremental load detection
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        SHA256 hash as hex string
    """
    sha256_hash = hashlib.sha256()
    
    try:
        with open(file_path, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b''):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error computing file hash: {str(e)}")
        raise


def read_csv_file(file_path: str) -> Generator[Dict[str, str], None, None]:
    """
    Read CSV file and yield rows as dictionaries
    
    Args:
        file_path: Path to CSV file
        
    Yields:
        Dictionary with column headers as keys
    """
    path = Path(file_path)
    
    if not path.exists():
        logger.error(f"CSV file not found: {file_path}")
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    logger.info(f"Reading CSV file: {file_path}")
    
    try:
        with open(path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            if reader.fieldnames is None:
                logger.error("CSV file is empty")
                raise ValueError("CSV file is empty")
            
            logger.info(f"CSV columns: {reader.fieldnames}")
            
            for row_num, row in enumerate(reader, start=2):  # start=2 because header is row 1
                yield {
                    "row_number": row_num,
                    "data": row
                }
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def count_csv_rows(file_path: str) -> int:
    """Count total rows in CSV file (excluding header)"""
    path = Path(file_path)
    count = 0
    try:
        with open(path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for _ in reader:
                count += 1
        return count
    except Exception as e:
        logger.error(f"Error counting CSV rows: {str(e)}")
        return 0
