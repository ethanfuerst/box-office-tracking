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

Create a `.env` file in the project root with the following environment variables:

```bash
S3_BUCKET="Your S3 bucket name"
S3_ACCESS_KEY_ID="Your S3 access key ID"
S3_SECRET_ACCESS_KEY="Your S3 secret access key"
S3_ENDPOINT="Your S3 endpoint"
S3_REGION="Your S3 region"
```

The `.env` file is automatically loaded when running the application locally. For Modal deployment, configure these as secrets in Modal.

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

By default, the job is scheduled to run daily at 07:00 UTC (configured via `modal.Cron('0 7 * * *')`).

## Versioning and Published Tables

This project uses semantic versioning (`MAJOR.MINOR.PATCH`).
Breaking changes to the published schema bump the major version.
Each major version writes to a separate S3 prefix, for example, `published_tables/v1/` and `published_tables/v2/`.
Downstream consumers (for example, [box-office-drafting](https://github.com/ethanfuerst/box-office-drafting)) should pin to a compatible major version and S3 prefix.

## License

This project is licensed under the MIT license. See [LICENSE](LICENSE) for details.
