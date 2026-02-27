# CSV to PostgreSQL ETL Pipeline

Production-grade data ingestion system for loading CSV files into PostgreSQL with validation, transformation, and audit logging.

![Python](https://img.shields.io/badge/python-3.11+-blue?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/postgresql-16+-blue?style=flat-square)
![Docker](https://img.shields.io/badge/docker-compose-blue?style=flat-square)
![SQLAlchemy](https://img.shields.io/badge/sqlalchemy-2.0+-green?style=flat-square)
![Tests](https://github.com/Mithileshan/csv-to-postgres-etl-pipeline/actions/workflows/test.yml/badge.svg)
![Lint](https://github.com/Mithileshan/csv-to-postgres-etl-pipeline/actions/workflows/lint.yml/badge.svg)

---

## Overview

A scalable, production-ready ETL (Extract-Transform-Load) pipeline that:

- **Extracts** CSV data from `/data/inbox` folder
- **Validates** data quality with configurable rules
- **Transforms** raw data to target schema
- **Loads** into PostgreSQL with full audit trail
- **Logs** every run with row counts and error tracking
- **Isolates** bad records for analysis

Perfect for portfolio demonstrations of:
- Python data engineering
- Database design & optimization
- Docker containerization
- Logging & monitoring
- Error handling & data quality

---

## Architecture

```
CSV File (inbox/)
    ↓
Extract (CSV reader)
    ↓
Validate (Data quality checks)
    ↓
Transform (Schema mapping)
    ↓
Load (PostgreSQL)
    ├── staging_users (raw)
    ├── users (clean)
    ├── etl_runs (audit)
    └── etl_bad_rows (errors)
```

---

## Project Structure

```
csv-to-postgres-etl/
├── etl/
│   ├── __init__.py              # Package initialization
│   ├── config.py                # Pydantic settings
│   ├── models.py                # SQLAlchemy ORM models
│   ├── extract.py               # CSV reading
│   ├── validate.py              # Data quality rules
│   ├── transform.py             # Schema transformation
│   ├── load.py                  # Database operations
│   ├── run.py                   # Main orchestration
│   ├── requirements.txt          # Python dependencies
│   └── Dockerfile               # Container definition
│
├── data/
│   ├── inbox/                   # Input CSV files
│   │   └── users.csv            # Sample data
│   └── bad_rows/                # Failed records for analysis
│
├── docker-compose.yaml          # Multi-container orchestration
├── .env.example                 # Environment template
├── .env                         # Local configuration
└── README.md                    # This file
```

---

## Database Schema

### `staging_users` Table
Raw CSV data before validation:
- `id` (PK, auto-increment)
- `raw_data` (JSON of CSV row)
- `created_at` (timestamp)
- `etl_run_id` (FK to etl_runs)

### `users` Table
Clean, validated user data:
- `id` (PK)
- `name` (String)
- `email` (Unique)
- `country` (String)
- `created_at` (from CSV)
- `inserted_at` (timestamp)
- `updated_at` (timestamp)

### `etl_runs` Table
Audit log for each pipeline execution:
- `id` (PK)
- `run_date` (timestamp)
- `rows_processed` (count)
- `rows_loaded` (count)
- `rows_failed` (count)
- `status` (SUCCESS/FAILED)
- `error_message` (text)
- `duration_seconds` (int)

### `etl_bad_rows` Table
Records that failed validation:
- `id` (PK)
- `etl_run_id` (FK)
- `raw_row` (JSON)
- `error_reason` (string)
- `created_at` (timestamp)

---

## Environment Configuration

### `.env` File

```env
# PostgreSQL Connection
DATABASE_URL=postgresql://user:pass@host:5432/dbname
DATABASE_HOST=db
DATABASE_PORT=5432
DATABASE_NAME=etl_db
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres

# ETL Paths
CSV_INPUT_PATH=/app/data/inbox/users.csv
BAD_ROWS_PATH=/app/data/bad_rows

# Logging
LOG_LEVEL=INFO
```

---

## Quick Start

### With Docker Compose (Recommended)

```bash
# Clone repository
git clone https://github.com/Mithileshan/csv-to-postgres-etl-pipeline.git
cd csv-to-postgres-etl-pipeline

# Build and start services
docker compose up -d --build

# Run ETL pipeline
docker compose run etl python -m etl.run
```

### Local Development

```bash
# Install dependencies
pip install -r elt/requirements.txt

# Configure .env
cp .env.example .env
# Edit .env for your local PostgreSQL

# Run pipeline
python -m etl.run
```

---

## Usage

### Run ETL Pipeline

```bash
# Docker
docker compose run etl python -m etl.run

# Local
python -m etl.run
```

### Access PostgreSQL

```bash
# From Docker container
docker exec -it etl_postgres psql -U postgres -d etl_db

# Local connection
psql -h localhost -p 5433 -U postgres -d etl_db
```

### Query Results

```sql
-- See all ETL runs
SELECT * FROM etl_runs ORDER BY run_date DESC;

-- See loaded users
SELECT * FROM users;

-- See failed rows
SELECT * FROM etl_bad_rows WHERE etl_run_id = 1;

-- Get run statistics
SELECT 
  id, 
  run_date, 
  rows_processed, 
  rows_loaded, 
  rows_failed, 
  status,
  duration_seconds
FROM etl_runs
ORDER BY run_date DESC
LIMIT 10;
```

---

## Phase 3: Production-Grade Data Quality & Incremental Processing

Phase 3 implements production-grade features for enterprise ETL reliability:

### 3.1 Strict Data Validation

All CSV rows undergo strict validation with detailed error reporting:

#### Email Validation
- RFC-compliant regex pattern validation
- Maximum length: 255 characters
- Uniqueness check within batch and existing data

#### Country Code Validation
- ISO 3166-1 alpha-2 standard codes only
- 48 supported countries (US, GB, CA, AU, NZ, SG, MX, DE, FR, etc.)
- Real-time validation with clear error messages

#### Name Validation
- Minimum length: 2 characters
- Maximum length: 255 characters
- Supports international characters

#### Date Format Flexibility
- Auto-detects multiple date formats:
  - `YYYY-MM-DD`
  - `YYYY-MM-DD HH:MM:SS`
  - `YYYY/MM/DD`
  - `MM-DD-YYYY`
- Graceful error reporting for invalid dates

### 3.2 Data Quality Gates

Configurable quality thresholds prevent corrupted data loads:

```env
# Quality Gate Configuration (.env)
MAX_INVALID_RATIO=0.05              # Fail if >5% of rows invalid
MAX_DUPLICATE_EMAILS=0              # Zero tolerance for duplicates
FAIL_ON_DUPLICATE_EMAILS=true       # Fail rather than warn
STRICT_MODE=true                    # Fail immediately on validation error
```

#### Invalid Ratio Gate
```
Total Rows: 1000
Invalid Rows: 45 (4.5%)
STATUS: ✅ PASS (4.5% < 5% threshold)

Total Rows: 1000
Invalid Rows: 65 (6.5%)
STATUS: ❌ FAIL (6.5% > 5% threshold)
```

#### Duplicate Email Gate
```
Duplicate Emails Detected: 0
STATUS: ✅ PASS

Duplicate Emails Detected: 2
STATUS: ❌ FAIL (duplicates > threshold of 0)
```

**Pipeline behavior when gate fails:**
- No data loaded to users table
- Bad rows recorded for analysis
- ETL run marked as FAILED
- Error message captured for audit
- Enable `STRICT_MODE=false` to warn instead of fail

### 3.3 Incremental Load Watermarking

SHA256 file hashing prevents duplicate processing:

```python
# How it works:

# First Run
1. Compute file hash: SHA256(users.csv) = "abc123...def456"
2. File not in DB → proceed with loading
3. Load all rows
4. Create ETL run with file_hash = "abc123...def456", status = SUCCESS

# Second Run (Same File)
1. Compute file hash: SHA256(users.csv) = "abc123...def456"
2. Query: SELECT * FROM etl_runs WHERE file_hash='abc123' AND status='SUCCESS'
3. Found existing record → SKIP (file already processed)
4. Create ETL run with status = SKIPPED
```

**SQL to verify watermarking:**

```sql
-- See all runs including skipped ones
SELECT 
  id,
  run_date,
  file_hash,
  source_file,
  rows_processed,
  rows_loaded,
  status,
  duration_seconds
FROM etl_runs
ORDER BY run_date DESC;

-- Find skipped runs (duplicate files)
SELECT * FROM etl_runs WHERE status = 'SKIPPED';

-- Audit trail for specific file
SELECT * FROM etl_runs WHERE file_hash = 'abc123...def456' ORDER BY run_date DESC;
```

### 3.4 Idempotent Upsert Strategy

PostgreSQL `ON CONFLICT` ensures safe repeat operations:

```sql
-- Upsert logic (INSERT or UPDATE on email conflict)
INSERT INTO users (id, name, email, country, created_at, inserted_at, updated_at)
VALUES (1, 'Alice', 'alice@example.com', 'US', '2024-01-01', NOW(), NOW())
ON CONFLICT (email) DO UPDATE SET
  name = EXCLUDED.name,
  country = EXCLUDED.country,
  updated_at = NOW()
WHERE users.email = EXCLUDED.email;
```

**Example scenario:**

```
Initial Load
INSERT: Alice (alice@example.com) → New record created
  id=1, name='Alice', email='alice@example.com', country='US'
  created_at='2024-01-01', inserted_at=NOW(), updated_at=NOW()

Update Same Email
INSERT: Alice Updated (alice@example.com) → Email conflict!
  ON CONFLICT triggers UPDATE:
  name='Alice Updated', country='CA', updated_at=NOW()
  
  Result: Same record (id=1) updated
  id=1, name='Alice Updated', email='alice@example.com', country='CA'
  created_at='2024-01-01' (PRESERVED), inserted_at=original, updated_at=NOW()

Query Results
SELECT * FROM users WHERE email='alice@example.com';
  → Only ONE row (not duplicated)
  → Shows latest name and country
  → created_at unchanged (original submission)
  → updated_at reflects latest change
```

### 3.5 Comprehensive Run Metrics

Every ETL run tracked with detailed statistics:

```python
etl_runs table fields:
- id                  # Unique run identifier
- run_date            # When run executed
- file_hash           # SHA256 of input file (unique constraint)
- source_file         # Source file path
- rows_processed      # Total rows in CSV
- rows_loaded         # Rows successfully inserted/updated
- rows_failed         # Rows failed validation
- rows_skipped        # Rows skipped (from deduplication)
- status              # IN_PROGRESS, SUCCESS, FAILED, SKIPPED
- error_message       # Detailed error if FAILED
- duration_seconds    # Pipeline execution time
```

### 3.6 Testing & Validation

Comprehensive pytest test suite validates all Phase 3 features:

```bash
# Run full test suite
pytest elt/tests/test_phase3.py -v

# Run specific test class
pytest elt/tests/test_phase3.py::TestEmailValidation -v

# Run with coverage
pytest elt/tests/test_phase3.py --cov=elt --cov-report=html

# Test categories:
# - TestEmailValidation          (email format & length)
# - TestCountryValidation        (ISO 3166 codes)
# - TestNameValidation           (2-255 char bounds)
# - TestDateValidation           (multi-format parsing)
# - TestDuplicateDetection       (email uniqueness)
# - TestRowValidation            (complete row checks)
# - TestFileHash                 (SHA256 watermarking)
# - TestIncrementalLoadWatermark (skip logic)
# - TestQualityGateInvalidRatio  (5% threshold)
# - TestQualityGateDuplicateEmails (zero tolerance)
# - TestUpsertUser               (idempotent loading)
# - TestConfigSettings           (verification)
```

### 3.7 Real-World Usage Examples

**Scenario 1: Valid CSV, First Run**
```bash
docker compose run etl python -m etl.run

[2024-01-15 10:30:00] Starting CSV to PostgreSQL ETL Pipeline (Strict Mode: True)
[2024-01-15 10:30:00] [1/6] Initializing database...
[2024-01-15 10:30:01] [2/6] Computing file hash for incremental load detection...
[2024-01-15 10:30:01] File hash: a1b2c3d4e5f6...
[2024-01-15 10:30:01] [3/6] File is new - proceeding with full pipeline
[2024-01-15 10:30:02] [4/6] Creating ETL run record...
[2024-01-15 10:30:02] [5/6] Reading and validating CSV rows...
[2024-01-15 10:30:05] Validation complete: 100 valid, 0 invalid
[2024-01-15 10:30:05] [6/6] Applying quality gates...
[2024-01-15 10:30:05] ✅ All quality gates passed
[2024-01-15 10:30:08] ✅ ETL Pipeline Complete - SUCCESS
[2024-01-15 10:30:08] Total Rows Processed:  100
[2024-01-15 10:30:08] Rows Valid:            100
[2024-01-15 10:30:08] Rows Invalid:          0
[2024-01-15 10:30:08] Rows Loaded:           100
[2024-01-15 10:30:08] Rows Failed:           0
[2024-01-15 10:30:08] Duration:              8 seconds

Result: etl_runs shows status='SUCCESS', file_hash set, rows_loaded=100
```

**Scenario 2: Same File, Second Run (Duplicate Detected)**
```bash
docker compose run etl python -m etl.run

[2024-01-15 10:35:00] Starting CSV to PostgreSQL ETL Pipeline (Strict Mode: True)
[2024-01-15 10:35:00] [1/6] Initializing database...
[2024-01-15 10:35:00] [2/6] Computing file hash for incremental load detection...
[2024-01-15 10:35:00] File hash: a1b2c3d4e5f6... (same as before)
[2024-01-15 10:35:00] [3/6] File already processed successfully - SKIPPING

Result: etl_runs shows status='SKIPPED', rows_processed=0, rows_loaded=0
```

**Scenario 3: CSV with >5% Invalid Rows**
```bash
# CSV has 110 rows, 8 invalid (7.3% > 5% threshold)

[2024-01-15 10:40:00] Validation complete: 102 valid, 8 invalid
[2024-01-15 10:40:00] Invalid row ratio: 7.3% (threshold: 5.0%)
[2024-01-15 10:40:00] ❌ QUALITY GATE FAILED: Invalid ratio 7.3% exceeds threshold 5.0%
[2024-01-15 10:40:01] ETL Pipeline Failed - Quality Gate Violation

Result: etl_runs shows status='FAILED', error_message captured
         etl_bad_rows populated with 8 rows for analysis
         users table NOT updated (fail-fast behavior)
```

**Scenario 4: Duplicate Emails Detected**
```bash
# CSV has duplicate email addresses (alice@example.com appears twice)

[2024-01-15 10:45:00] Validation complete: 100 valid, 0 invalid
[2024-01-15 10:45:00] ❌ QUALITY GATE FAILED: Found 1 duplicate emails (threshold: 0)
[2024-01-15 10:45:00] ETL Pipeline Failed - Duplicate Email Detection

Result: etl_runs shows status='FAILED'
         No data loaded (fail-fast)
         Manual investigation required
```

---

## Phase 4–7: CI/CD & Production Operations

### GitHub Actions Automation (Phase 7)

All code changes are automatically tested and linted:

#### Tests (`test.yml`)
- ✅ Runs on every push to `main` and `develop`
- ✅ Runs on all pull requests
- ✅ Executes full pytest suite in PostgreSQL container
- ✅ Generates coverage report
- ✅ Fails build if tests don't pass

```bash
# See test results: GitHub Actions tab → "Run Tests" workflow
```

#### Lint (`lint.yml`)
- ✅ Code quality checks with flake8
- ✅ Format validation with black
- ✅ Import sorting with isort
- ✅ Runs on every push and PR
- ✅ Warnings don't block merge (informational only)

```bash
# Local lint check before pushing:
flake8 elt/ --max-complexity=10
black elt/ --check
isort elt/ --check-only
```

### Local Development Workflow

```bash
# 1. Install dev dependencies
pip install -r elt/requirements.txt

# 2. Run tests locally
pytest elt/tests/test_phase3.py -v --cov=elt

# 3. Format code before commit
black elt/
isort elt/

# 4. Check for issues
flake8 elt/ --max-complexity=10

# 5. Commit & push (CI will run automatically)
git add -A
git commit -m "feat: description"
git push origin main
```

### CI Status Dashboard

1. Go to: `https://github.com/Mithileshan/csv-to-postgres-etl-pipeline/actions`
2. View:
   - ✅ **Run Tests** - Test results + coverage
   - ✅ **Lint & Code Quality** - Flake8 + Black + Isort

### Production Deployment Readiness

| Area | Status | Details |
|------|--------|---------|
| **Testing** | ✅ Automated | 12 test classes, pytest-cov reporting |
| **Code Quality** | ✅ Automated | flake8, black, isort checks |
| **Database** | ✅ Tested | PostgreSQL 16.2 in CI environment |
| **Docker** | ✅ Ready | Multi-stage build, health checks |
| **Documentation** | ✅ Complete | README + inline code comments |
| **Monitoring** | ✅ Built-in | etl_runs audit table + error_reason tracking |

---

## Phases Complete

- **Phase 1** (Completed): Repository setup, Docker baseline
- **Phase 2** (Completed): Production schema, idempotent CSV loading, audit logging
- **Phase 3** (Completed): Data validation rules, quality gates, incremental loads, upsert strategy
- **Phase 4–7** (Completed): GitHub Actions CI/CD, code quality automation, production-ready deployment

---

## Development

### Adding New Validation Rules (Phase 3)

Edit `elt/validate.py`:

```python
def validate_row(row: Dict[str, str]) -> Tuple[bool, str]:
    # Add email format check
    # Add country lookup
    # Add date parsing
    # etc.
```

### Adding New Transformations

Edit `elt/transform.py`:

```python
def transform_user_row(raw_row: Dict[str, str]) -> Dict[str, str]:
    # Add field parsing
    # Add normalization
    # Add enrichment
    # etc.
```

### Monitoring

Check Docker logs:

```bash
docker compose logs -f etl
docker compose logs -f db
```

---

## Performance

- **Batch Size**: Configurable (default: process all rows)
- **Logging Interval**: Every 100 rows
- **Database**: Indexed on email (unique constraint)
- **Parallelization**: Ready for phase 3 (multi-worker pattern)

---

## Security

- Environment-based credentials (no hardcoding)
- Parameterized queries (SQLAlchemy ORM)
- Input validation before loading
- Audit trail of all loads
- Bad row isolation for analysis

---

## Author

[Mithileshan](https://github.com/Mithileshan)

---

## License

MIT License - See LICENSE file for details
