"""
Configuration management using Pydantic Settings
Environment variables loaded from .env file
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application configuration from environment variables"""
    
    # Database
    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/etl_db"
    DATABASE_HOST: str = "db"
    DATABASE_PORT: int = 5432
    DATABASE_NAME: str = "etl_db"
    DATABASE_USER: str = "postgres"
    DATABASE_PASSWORD: str = "postgres"
    
    # ETL
    CSV_INPUT_PATH: str = "/app/data/inbox/users.csv"
    BAD_ROWS_PATH: str = "/app/data/bad_rows"
    
    # Quality Gates (Phase 3)
    MAX_INVALID_RATIO: float = 0.05  # Fail if >5% of rows are invalid
    MAX_DUPLICATE_EMAILS: int = 0    # Fail if duplicates > this threshold
    FAIL_ON_DUPLICATE_EMAILS: bool = True  # Stop pipeline if duplicate emails found
    STRICT_MODE: bool = True  # If False, warnings instead of failures
    
    # Dataset
    DATASET_NAME: str = "users"
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


def get_settings() -> Settings:
    """Get application settings singleton"""
    return Settings()


settings = get_settings()
