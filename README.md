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

