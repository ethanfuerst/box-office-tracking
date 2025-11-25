# box-office-tracking

> Creates a cron job using [Modal](https://modal.com/) to scrape worldwide box office data from Box Office Mojo, process it with DuckDB, and publish cleaned tables to S3.

## Architecture
Data flows as scraped from Box Office Mojo → processed with DuckDB → published to S3. Raw data is partitioned by `release_year` and `scraped_date` in Parquet format. Cleaned tables are publised to the `published_tables/` path.

## Tables Published
- `daily_ranks`: One row per film per scraped date.

See [SCHEMA.md](SCHEMA.md) for full column definitions and version history.

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

### Local Development

```bash
# Run locally
uv run python3 app.py
```

### Deployment

The application runs on Modal with daily scheduled updates at 9 AM UTC. All config files in `src/config/` are automatically discovered and processed.

```bash
uv run modal deploy app.py
```

## Versioning & Published Tables
Versions will change the published tables, partitioned by major version.
