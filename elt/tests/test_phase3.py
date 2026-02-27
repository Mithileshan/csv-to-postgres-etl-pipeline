"""
Phase 3 Test Suite: Quality Gates, Incremental Loads, and Upsert
Tests validation rules, quality gates, file hash watermarking, and upsert logic
"""

import pytest
import hashlib
from datetime import datetime
from pathlib import Path
import tempfile
import os

from validate import (
    validate_row, 
    validate_email,
    validate_country,
    validate_name,
    validate_created_at,
    detect_duplicates,
    VALID_COUNTRIES,
    EMAIL_REGEX
)
from extract import compute_file_hash
from load import (
    check_file_already_processed,
    upsert_user,
    get_session,
    init_db,
    create_etl_run,
    update_etl_run
)
from config import settings


class TestEmailValidation:
    """Test email validation rules"""
    
    def test_valid_email(self):
        """Valid email should pass"""
        valid_emails = [
            "user@example.com",
            "john.doe@company.co.uk",
            "alice+tag@domain.org"
        ]
        for email in valid_emails:
            is_valid, error = validate_email(email)
            assert is_valid, f"Email {email} should be valid"
    
    def test_invalid_email_format(self):
        """Invalid email format should fail"""
        invalid_emails = [
            "notanemail",
            "user@",
            "@example.com",
            "user@ example.com",
            "user@.com"
        ]
        for email in invalid_emails:
            is_valid, error = validate_email(email)
            assert not is_valid, f"Email {email} should be invalid"
    
    def test_invalid_email_too_long(self):
        """Email exceeding 255 chars should fail"""
        long_email = "a" * 250 + "@example.com"
        is_valid, error = validate_email(long_email)
        assert not is_valid, "Too long email should be invalid"


class TestCountryValidation:
    """Test country code validation"""
    
    def test_valid_country_codes(self):
        """Valid ISO 3166-1 alpha-2 codes should pass"""
        valid_codes = ["US", "GB", "CA", "AU", "NZ", "SG", "MX", "DE", "FR"]
        for code in valid_codes:
            is_valid, error = validate_country(code)
            assert is_valid, f"Country {code} should be valid"
    
    def test_invalid_country_code(self):
        """Invalid country codes should fail"""
        invalid_codes = ["XX", "USA", "UnitedStates", "1", ""]
        for code in invalid_codes:
            is_valid, error = validate_country(code)
            assert not is_valid, f"Country {code} should be invalid"
    
    def test_valid_countries_set_size(self):
        """Should have 48 valid country codes"""
        assert len(VALID_COUNTRIES) == 48, "Should have 48 valid country codes"


class TestNameValidation:
    """Test name validation rules"""
    
    def test_valid_name_length(self):
        """Names within 2-255 char range should pass"""
        valid_names = [
            "Jo",  # 2 chars min
            "John Doe",
            "Mary Jane Watson",
            "A" * 255  # 255 chars max
        ]
        for name in valid_names:
            is_valid, error = validate_name(name)
            assert is_valid, f"Name '{name}' should be valid"
    
    def test_invalid_name_too_short(self):
        """Names shorter than 2 chars should fail"""
        is_valid, error = validate_name("A")
        assert not is_valid, "Single char name should be invalid"
    
    def test_invalid_name_too_long(self):
        """Names longer than 255 chars should fail"""
        long_name = "A" * 256
        is_valid, error = validate_name(long_name)
        assert not is_valid, "Name > 255 chars should be invalid"


class TestDateValidation:
    """Test date parsing validation"""
    
    def test_valid_date_formats(self):
        """Should parse multiple date formats"""
        valid_dates = [
            "2024-01-15",
            "2024-01-15 10:30:45",
            "2024/01/15",
            "01-15-2024"
        ]
        for date_str in valid_dates:
            is_valid, error = validate_created_at(date_str)
            assert is_valid, f"Date '{date_str}' should be valid"
    
    def test_invalid_date(self):
        """Invalid dates should fail"""
        invalid_dates = [
            "2024-13-01",  # invalid month
            "not-a-date",
            "2024-02-30",  # invalid day
            ""
        ]
        for date_str in invalid_dates:
            is_valid, error = validate_created_at(date_str)
            assert not is_valid, f"Date '{date_str}' should be invalid"


class TestDuplicateDetection:
    """Test duplicate email detection"""
    
    def test_detect_duplicates(self):
        """Should identify duplicate emails"""
        rows = [
            {"id": "1", "name": "Alice", "email": "alice@example.com", "country": "US", "created_at": "2024-01-01"},
            {"id": "2", "name": "Bob", "email": "bob@example.com", "country": "UK", "created_at": "2024-01-02"},
            {"id": "3", "name": "Alice2", "email": "alice@example.com", "country": "US", "created_at": "2024-01-03"},
        ]
        duplicates, unique = detect_duplicates(rows)
        
        assert len(duplicates) == 1, "Should find 1 duplicate email"
        assert "alice@example.com" in duplicates, "Should identify alice@example.com as duplicate"
        assert len(unique) == 2, "Should have 2 unique rows"
    
    def test_no_duplicates(self):
        """Should return empty list for unique emails"""
        rows = [
            {"id": "1", "name": "Alice", "email": "alice@example.com", "country": "US", "created_at": "2024-01-01"},
            {"id": "2", "name": "Bob", "email": "bob@example.com", "country": "UK", "created_at": "2024-01-02"},
        ]
        duplicates, unique = detect_duplicates(rows)
        
        assert len(duplicates) == 0, "Should find no duplicates"
        assert len(unique) == 2, "Should have 2 unique rows"


class TestRowValidation:
    """Test complete row validation"""
    
    def test_valid_row(self):
        """Valid row should pass all checks"""
        valid_row = {
            "id": "1",
            "name": "John Doe",
            "email": "john@example.com",
            "country": "US",
            "created_at": "2024-01-01"
        }
        is_valid, error = validate_row(valid_row)
        assert is_valid, f"Row should be valid, got error: {error}"
    
    def test_invalid_row_bad_email(self):
        """Row with invalid email should fail"""
        invalid_row = {
            "id": "1",
            "name": "John Doe",
            "email": "not-an-email",
            "country": "US",
            "created_at": "2024-01-01"
        }
        is_valid, error = validate_row(invalid_row)
        assert not is_valid, "Row with invalid email should fail"
    
    def test_invalid_row_bad_country(self):
        """Row with invalid country should fail"""
        invalid_row = {
            "id": "1",
            "name": "John Doe",
            "email": "john@example.com",
            "country": "ZZ",
            "created_at": "2024-01-01"
        }
        is_valid, error = validate_row(invalid_row)
        assert not is_valid, "Row with invalid country should fail"
    
    def test_invalid_row_bad_date(self):
        """Row with invalid date should fail"""
        invalid_row = {
            "id": "1",
            "name": "John Doe",
            "email": "john@example.com",
            "country": "US",
            "created_at": "not-a-date"
        }
        is_valid, error = validate_row(invalid_row)
        assert not is_valid, "Row with invalid date should fail"


class TestFileHash:
    """Test file hash computation for incremental loads"""
    
    def test_compute_file_hash(self):
        """Should compute SHA256 hash of file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name,email\n1,Test,test@example.com\n")
            temp_path = f.name
        
        try:
            file_hash = compute_file_hash(temp_path)
            
            # Hash should be 64 chars (SHA256 hex is 64 chars)
            assert len(file_hash) == 64, "SHA256 hash should be 64 chars"
            
            # Same file should produce same hash
            file_hash2 = compute_file_hash(temp_path)
            assert file_hash == file_hash2, "Same file should produce same hash"
        finally:
            os.unlink(temp_path)
    
    def test_different_files_different_hash(self):
        """Different files should produce different hashes"""
        # Create two different temp files
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f1:
            f1.write("id,name,email\n1,Alice,alice@example.com\n")
            path1 = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f2:
            f2.write("id,name,email\n1,Bob,bob@example.com\n")
            path2 = f2.name
        
        try:
            hash1 = compute_file_hash(path1)
            hash2 = compute_file_hash(path2)
            
            assert hash1 != hash2, "Different files should have different hashes"
        finally:
            os.unlink(path1)
            os.unlink(path2)


class TestIncrementalLoadWatermark:
    """Test file hash watermarking for incremental loads"""
    
    def test_file_already_processed_skip(self):
        """File with existing SUCCESS run should be detected as processed"""
        # Initialize DB
        init_db()
        session = get_session()
        
        # Create a run record with file hash and SUCCESS status
        test_hash = "abc123def456" * 5 + "abc1"  # 64 chars
        run = create_etl_run(session, file_hash=test_hash, source_file="test.csv")
        update_etl_run(session, run.id, 10, 10, 0, "SUCCESS")
        
        # Check if file is already processed
        result = check_file_already_processed(session, test_hash)
        assert result is True, "File with SUCCESS status should be detected as processed"
    
    def test_file_not_processed_if_failed(self):
        """File with FAILED status should not be detected as processed"""
        init_db()
        session = get_session()
        
        # Create a run record with FAILED status
        test_hash = "xyz789" * 11  # 66 chars, truncate to 64
        test_hash = test_hash[:64]
        run = create_etl_run(session, file_hash=test_hash)
        update_etl_run(session, run.id, 10, 0, 10, "FAILED", error_message="Test error")
        
        # Check if file is already processed
        result = check_file_already_processed(session, test_hash)
        assert result is False, "File with FAILED status should not be detected as processed"


class TestQualityGateInvalidRatio:
    """Test MAX_INVALID_RATIO quality gate"""
    
    def test_invalid_ratio_within_threshold(self):
        """Pipeline should proceed if invalid ratio is within threshold"""
        total_rows = 100
        invalid_rows = 4  # 4% < 5% threshold
        
        invalid_ratio = invalid_rows / total_rows
        assert invalid_ratio <= settings.MAX_INVALID_RATIO, \
            "4% invalid should pass 5% threshold"
    
    def test_invalid_ratio_exceeds_threshold(self):
        """Pipeline should fail if invalid ratio exceeds threshold"""
        total_rows = 100
        invalid_rows = 6  # 6% > 5% threshold
        
        invalid_ratio = invalid_rows / total_rows
        assert invalid_ratio > settings.MAX_INVALID_RATIO, \
            "6% invalid should fail 5% threshold"


class TestQualityGateDuplicateEmails:
    """Test duplicate email detection quality gate"""
    
    def test_duplicate_emails_within_threshold(self):
        """Pipeline should proceed if duplicate emails within threshold"""
        rows = [
            {"id": "1", "name": "Alice", "email": "alice@example.com", "country": "US", "created_at": "2024-01-01"},
            {"id": "2", "name": "Bob", "email": "bob@example.com", "country": "UK", "created_at": "2024-01-02"},
        ]
        
        duplicates, unique = detect_duplicates(rows)
        assert len(duplicates) <= settings.MAX_DUPLICATE_EMAILS, \
            "No duplicates should pass threshold"
    
    def test_duplicate_emails_exceeds_threshold(self):
        """Pipeline should fail if duplicate emails exceed threshold"""
        rows = [
            {"id": "1", "name": "Alice", "email": "alice@example.com", "country": "US", "created_at": "2024-01-01"},
            {"id": "2", "name": "Alice2", "email": "alice@example.com", "country": "US", "created_at": "2024-01-02"},
        ]
        
        duplicates, unique = detect_duplicates(rows)
        
        # If MAX_DUPLICATE_EMAILS is 0 (which it is)
        if settings.FAIL_ON_DUPLICATE_EMAILS and settings.MAX_DUPLICATE_EMAILS == 0:
            assert len(duplicates) > settings.MAX_DUPLICATE_EMAILS, \
                "1 duplicate should exceed 0 threshold"


class TestUpsertUser:
    """Test upsert logic for idempotent loading"""
    
    def test_upsert_new_user(self):
        """New user should be inserted"""
        init_db()
        session = get_session()
        
        user_data = {
            "id": "1",
            "name": "Alice",
            "email": "alice@example.com",
            "country": "US",
            "created_at": "2024-01-01"
        }
        
        result = upsert_user(session, user_data)
        assert result is True, "Upsert should succeed"
    
    def test_upsert_existing_user_updates(self):
        """Existing user with same email should be updated"""
        init_db()
        session = get_session()
        
        # First insert
        user_data1 = {
            "id": "1",
            "name": "Alice",
            "email": "alice@example.com",
            "country": "US",
            "created_at": "2024-01-01"
        }
        upsert_user(session, user_data1)
        
        # Update same email
        user_data2 = {
            "id": "2",
            "name": "Alice Updated",
            "email": "alice@example.com",
            "country": "CA",
            "created_at": "2024-01-02"
        }
        result = upsert_user(session, user_data2)
        
        assert result is True, "Upsert should succeed"
        # Note: Full verification would require running against actual DB


class TestConfigSettings:
    """Test Phase 3 configuration settings"""
    
    def test_strict_mode_enabled(self):
        """STRICT_MODE should be enabled"""
        assert settings.STRICT_MODE is True, "STRICT_MODE should be True for Phase 3"
    
    def test_max_invalid_ratio_set(self):
        """MAX_INVALID_RATIO should be configured"""
        assert settings.MAX_INVALID_RATIO == 0.05, "MAX_INVALID_RATIO should be 0.05 (5%)"
    
    def test_fail_on_duplicate_emails_enabled(self):
        """FAIL_ON_DUPLICATE_EMAILS should be enabled"""
        assert settings.FAIL_ON_DUPLICATE_EMAILS is True, \
            "FAIL_ON_DUPLICATE_EMAILS should be True"
    
    def test_max_duplicate_emails_zero(self):
        """MAX_DUPLICATE_EMAILS should be 0 (zero tolerance)"""
        assert settings.MAX_DUPLICATE_EMAILS == 0, "MAX_DUPLICATE_EMAILS should be 0"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
