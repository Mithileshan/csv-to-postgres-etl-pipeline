"""
Transform phase: Convert raw data to target schema
"""

import logging
from typing import Dict

logger = logging.getLogger(__name__)


def transform_user_row(raw_row: Dict[str, str]) -> Dict[str, str]:
    """
    Transform raw CSV row to user table format
    
    Args:
        raw_row: Raw data from CSV
        
    Returns:
        Transformed row ready for loading
    """
    return {
        "id": raw_row.get("id", "").strip(),
        "name": raw_row.get("name", "").strip(),
        "email": raw_row.get("email", "").strip().lower(),
        "country": raw_row.get("country", "").strip(),
        "created_at": raw_row.get("created_at", "").strip(),
    }
