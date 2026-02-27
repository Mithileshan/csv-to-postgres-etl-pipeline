"""
Validate phase: Data quality checks with strict schema validation
"""

import logging
import re
from typing import Dict, Tuple, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Valid ISO 3166-1 alpha-2 country codes
VALID_COUNTRIES = {
    'US', 'GB', 'CA', 'AU', 'NZ', 'SG', 'MX', 'IN', 'JP', 'DE', 'FR', 'IT', 'ES',
    'BR', 'CN', 'KR', 'RU', 'ZA', 'UA', 'PL', 'SE', 'NO', 'DK', 'NL', 'BE', 'CH',
    'AT', 'CZ', 'HU', 'RO', 'TR', 'GR', 'PT', 'IE', 'IL', 'SA', 'AE', 'NG', 'KE',
    'EG', 'VN', 'TH', 'MY', 'PH', 'ID', 'PK', 'IR', 'IQ'
}

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def validate_email(email: str) -> Tuple[bool, str]:
    """Validate email format"""
    if not email or not email.strip():
        return False, "Email is required"
    
    email = email.strip().lower()
    if not EMAIL_REGEX.match(email):
        return False, f"Invalid email format: {email}"
    
    if len(email) > 255:
        return False, "Email too long (max 255 chars)"
    
    return True, ""


def validate_country(country: str) -> Tuple[bool, str]:
    """Validate country code"""
    if not country or not country.strip():
        return False, "Country is required"
    
    country = country.strip().upper()
    if country not in VALID_COUNTRIES:
        return False, f"Invalid country code: {country} (must be ISO 3166-1 alpha-2)"
    
    return True, ""


def validate_name(name: str) -> Tuple[bool, str]:
    """Validate name field"""
    if not name or not name.strip():
        return False, "Name is required"
    
    name = name.strip()
    if len(name) < 2:
        return False, "Name too short (min 2 chars)"
    
    if len(name) > 255:
        return False, "Name too long (max 255 chars)"
    
    return True, ""


def validate_created_at(created_at: str) -> Tuple[bool, str]:
    """Validate created_at timestamp"""
    if not created_at or not created_at.strip():
        return True, ""  # Optional - will default to now
    
    created_at = created_at.strip()
    
    # Try common date formats
    formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%m/%d/%Y']
    
    for fmt in formats:
        try:
            datetime.strptime(created_at, fmt)
            return True, ""
        except ValueError:
            continue
    
    return False, f"Invalid date format: {created_at} (try YYYY-MM-DD)"


def validate_row(row: Dict[str, str]) -> Tuple[bool, str]:
    """
    Validate a single row of data with strict schema rules
    
    Args:
        row: Dictionary with user data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['id', 'name', 'email', 'country', 'created_at']
    
    # Check all required fields are present
    for field in required_fields:
        if field not in row:
            return False, f"Missing required field: {field}"
    
    # Validate name
    is_valid, error = validate_name(row.get('name', ''))
    if not is_valid:
        return False, error
    
    # Validate email
    is_valid, error = validate_email(row.get('email', ''))
    if not is_valid:
        return False, error
    
    # Validate country
    is_valid, error = validate_country(row.get('country', ''))
    if not is_valid:
        return False, error
    
    # Validate created_at (optional but if present must be valid)
    is_valid, error = validate_created_at(row.get('created_at', ''))
    if not is_valid:
        return False, error
    
    return True, ""


def detect_duplicates(valid_rows: List[Dict[str, str]]) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Detect duplicate emails in valid rows
    
    Args:
        valid_rows: List of validated rows
        
    Returns:
        Tuple of (duplicate_emails, unique_rows)
    """
    seen_emails = set()
    unique_rows = []
    duplicate_emails = []
    
    for row in valid_rows:
        email = row.get('email', '').strip().lower()
        if email in seen_emails:
            duplicate_emails.append(email)
        else:
            seen_emails.add(email)
            unique_rows.append(row)
    
    return duplicate_emails, unique_rows
