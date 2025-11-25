# Schema Documentation

Below are the published tables and their schemas.

## daily_ranks

**Location**: `s3://{bucket}/published_tables/daily_ranks/<version_number>/data.parquet`

**Grain**: One row per film per scraped date

**Description**: Aggregated worldwide box office data with standardized revenue fields and metadata.

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `title` | string | Film title |
| `revenue` | integer | Worldwide revenue in USD |
| `domestic_rev` | integer | Domestic (US) revenue in USD |
| `foreign_rev` | integer | Foreign (international) revenue in USD |
| `loaded_date` | date | Date when data was scraped from Box Office Mojo |
| `release_year` | integer | Year of film release |
| `published_timestamp_utc` | timestamp | UTC timestamp when the table was published |

### Version History

- **v1** (current): Initial schema with stable column definitions
