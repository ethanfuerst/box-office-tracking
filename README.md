# box-office-tracking

> Scrapes worldwide box office data from Box Office Mojo, processes it with DuckDB, and publishes cleaned tables to S3.

## Features
- Daily scraping of worldwide box office data for current and previous year
- Automatic data cleaning and transformation (revenue parsing, date handling)
- S3 storage with partitioning by release year and scraped date
- Published aggregated tables for downstream consumption
- Scheduled execution via Modal with automatic retries

## Architecture
Data flows from Box Office Mojo HTML tables → DuckDB for processing → S3 storage. Raw data is stored partitioned by `release_year` and `scraped_date` in Parquet format. A daily aggregation process reads all raw data and publishes cleaned tables to `published_tables/daily_ranks/data.parquet` with standardized columns and data types.

## Tables Published
- `daily_ranks`: One row per film per scraped date.

See [SCHEMA.md](SCHEMA.md) for full column definitions and version history.

## Installation
```bash
git clone <repository-url>
cd box-office-tracking
uv sync
```

## Configuration
Create `src/config/s3_sync.yml`:
```yaml
bucket: your-s3-bucket-name
s3_access_key_id_var_name: BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID
s3_secret_access_key_var_name: BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY
```

Set environment variables:
- `BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID`: S3 access key ID
- `BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY`: S3 secret access key

## Usage
Run locally:
```bash
python app.py
```

Run via Modal:
```bash
modal deploy app.py
```

## Scheduling / Production
The pipeline runs daily at 7:00 AM UTC via Modal's cron scheduler (`0 7 * * *`). The function includes automatic retries (max 3 attempts) with exponential backoff. Secrets are managed through Modal's secret management system (`box-office-tracking-secrets`).

## Versioning & Published Tables
Current version (0.1.0) writes to the `published_tables/daily_ranks/` path. The schema is stable and backward-compatible. Downstream consumers can rely on consistent column names and data types.

## Development
Format code:
```bash
black src/
isort src/
```

The project uses DuckDB for data processing, pandas for HTML parsing, and Modal for cloud execution. All database connections use context managers for automatic cleanup.

## License
MIT License. Note: Data is scraped from Box Office Mojo; ensure compliance with their terms of service.
