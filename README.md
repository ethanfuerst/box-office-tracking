# box-office-tracking

Creates a scheduled job on [Modal](https://modal.com/) to scrape worldwide box office data from Box Office Mojo, process it with DuckDB, and publish cleaned tables to S3 for downstream use (for example, `box-office-drafting`).

## Features

- Daily scrape of worldwide box office data from Box Office Mojo
- Raw Parquet data partitioned for efficient querying
- Analytics-ready “published” tables in S3
- Designed as a stable data source for other services and apps

## Architecture

High-level flow:

1. Scrape Box Office Mojo into raw Parquet files.
2. Process and aggregate data with DuckDB.
3. Write cleaned tables to S3 under a versioned `published_tables/` prefix.

Raw data is partitioned by `release_year` and `scraped_date`. Published tables are written to `published_tables/v{MAJOR}/...`.

## Published Tables

- `daily_ranks`: one row per film per scraped date.

See [SCHEMA.md](SCHEMA.md) for full column definitions and version history.

## Installation

```bash
git clone https://github.com/ethanfuerst/box-office-tracking.git
cd box-office-tracking
uv sync
```

## Configuration
Create src/config/s3_sync.yml:

```yaml
bucket: your-s3-bucket-name
s3_access_key_id_var_name: BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID
s3_secret_access_key_var_name: BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY
```

Set environment variables:
`BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID` - S3 access key ID
`BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY` - S3 secret access key

## Usage

### Local development

Run the app locally (no Modal schedule):

```bash
uv run python app.py
```

### Modal deployment

Deploy the scheduled job to Modal:

```bash
uv run modal deploy app.py
```

By default, the job is scheduled to run daily at 09:00 UTC. All config files in src/config/ are automatically discovered and processed.

## Versioning and Published Tables

This project uses semantic versioning (`MAJOR.MINOR.PATCH`).
Breaking changes to the published schema bump the major version.
Each major version writes to a separate S3 prefix, for example, `published_tables/v1/` and `published_tables/v2/`.
Downstream consumers (for example, [box-office-drafting](https://github.com/ethanfuerst/box-office-drafting)) should pin to a compatible major version and S3 prefix.

## License

This project is licensed under the MIT license. See [LICENSE](LICENSE) for details.
