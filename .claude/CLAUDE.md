# box-office-tracking

Scrapes worldwide box office data from Box Office Mojo daily, processes it with DuckDB/SQLMesh, and publishes analytics-ready tables to S3. Runs as a scheduled Modal job. Downstream consumers (like `box-office-drafting`) pin to versioned S3 prefixes for stable data.

## Key directories and files

- [app.py](app.py) - Modal entrypoint; defines the scheduled job and pipeline orchestration
- [src/etl/](src/etl/) - Extract, transform, load modules
  - [src/etl/extract/](src/etl/extract/) - scrapes Box Office Mojo into raw Parquet
  - [src/etl/transform/](src/etl/transform/) - runs SQLMesh transformations
  - [src/etl/load/](src/etl/load/) - publishes cleaned tables to S3
- [src/sqlmesh_project/](src/sqlmesh_project/) - SQLMesh models, macros, config
  - [models/](src/sqlmesh_project/models/) - layered SQL models (`_1_raw`, `_2_cleaned`, `_3_combined`, `_4_published`)
  - [config.py](src/sqlmesh_project/config.py) - DuckDB connection and S3 secrets
- [src/utils/](src/utils/) - shared utilities (logging, S3 helpers)
- [SCHEMA.md](SCHEMA.md) - published table schemas and version history
- [create_release.sh](create_release.sh) - interactive script to bump version, tag, and create GitHub releases

## Setup

```bash
git clone https://github.com/ethanfuerst/box-office-tracking.git
cd box-office-tracking
uv sync
```

Create `.env` with S3 credentials:
```bash
S3_BUCKET="your-bucket"
S3_ACCESS_KEY_ID="..."
S3_SECRET_ACCESS_KEY="..."
S3_ENDPOINT="..."
S3_REGION="..."
```

## Common commands

```bash
# Run the full pipeline locally (extract → transform → load)
uv run python app.py

# Skip extraction (useful if raw data already exists)
uv run python app.py --skip-extracts

# Force all extracts regardless of schedule logic
uv run python app.py --force-all-extracts

# Deploy to Modal (scheduled daily at 07:00 UTC)
uv run modal deploy app.py

# Format code
uv run isort .
uv run black .

# Run pre-commit hooks
pre-commit run --all-files

# Create a new release (interactive)
./create_release.sh
```

## Conventions and gotchas

- **Python 3.12 only** - `requires-python = ">=3.12,<3.13"`; do not upgrade to 3.13 yet
- **Skip string normalization** - use single quotes for strings in Python (configured in `pyproject.toml`)
- **Semantic versioning** - breaking schema changes bump MAJOR; each major version writes to `published_tables/v{MAJOR}/`
- **S3 secrets** - locally loaded via `.env`; on Modal, use `box-office-tracking-secrets` secret
- **Raw data partitioning** - raw Parquet is partitioned by `release_year` and `scraped_date`
- **DuckDB location** - local database stored in `src/duckdb_databases/box_office_tracking_sqlmesh_db.duckdb`
- **SQLMesh gateway** - uses `duckdb` gateway with httpfs extension and S3 secrets configured
- **Scheduled cron** - Modal job runs `0 7 * * *` (07:00 UTC daily)

## Where to start for common tasks

- **Add a new scraper** - create a new module in `src/etl/extract/tables/`, import in `src/etl/extract/main.py`
- **Update SQL transformations** - edit models in `src/sqlmesh_project/models/`; models are layered as `_1_raw`, `_2_cleaned`, `_3_combined`, `_4_published`
- **Change published schema** - update models in `_4_published/`, update [SCHEMA.md](SCHEMA.md), bump major version if breaking
- **Modify S3 paths or logic** - edit [src/utils/s3_utils.py](src/utils/s3_utils.py) and `src/etl/load/`
- **Change Modal schedule or config** - edit [app.py](app.py) (Modal image, schedule cron, retries)
- **Debug pipeline locally** - run `uv run python app.py` with optional flags; check `logs/` directory for output
