# Box Office Tracking - API Documentation

## Overview

The Box Office Tracking system is a comprehensive ETL (Extract, Transform, Load) pipeline designed to track fantasy box office drafts using data from Box Office Mojo. The system integrates with Google Sheets to create dynamic dashboards and supports both direct scraping and S3-based data storage.

## Table of Contents

- [Main Entry Points](#main-entry-points)
- [Dashboard ETL Module](#dashboard-etl-module)
- [Box Office ETL Module](#box-office-etl-module)
- [Utilities Module](#utilities-module)
- [Configuration](#configuration)
- [Data Models](#data-models)
- [Examples](#examples)

---

## Main Entry Points

### `sync_and_update.py`

The main orchestration script that provides Modal-based cloud functions and local execution capabilities.

#### Modal Functions

##### `s3_sync(ids: List[str] = DEFAULT_IDS)`

**Purpose**: Synchronizes box office data to S3 buckets for specified dashboard IDs.

**Schedule**: Runs daily at 4:00 AM UTC via Modal Cron

**Parameters**:
- `ids` (List[str], optional): List of dashboard IDs to sync. Defaults to all IDs from config.

**Example**:
```python
# Run locally
s3_sync.local(ids=["my_2025_draft"])

# Deploy to Modal (automatic scheduling)
deploy_app(app)
```

**Behavior**:
- Filters IDs to only those with `update_type: 's3'`
- Scrapes Box Office Mojo data for current and previous year
- Stores data in configured S3 buckets in Parquet format

##### `update_dashboards(ids: List[str] = DEFAULT_IDS, dry_run: bool = False)`

**Purpose**: Updates Google Sheets dashboards with latest box office data.

**Schedule**: Runs daily at 5:00 AM UTC via Modal Cron

**Parameters**:
- `ids` (List[str], optional): Dashboard IDs to update
- `dry_run` (bool): If True, skips Google Sheets loading

**Example**:
```python
# Update specific dashboards
update_dashboards.local(ids=["draft_2025"], dry_run=False)

# Dry run for testing
update_dashboards.local(ids=["draft_2025"], dry_run=True)
```

#### CLI Interface

```bash
# Update all dashboards locally
python sync_and_update.py

# Update specific dashboards
python sync_and_update.py --ids draft_2025 draft_2024

# Run S3 sync only
python sync_and_update.py --sync-s3

# Dry run (skip Google Sheets upload)
python sync_and_update.py --dry-run
```

---

## Dashboard ETL Module

### `dashboard_etl.extract`

#### `get_draft_data(config: Dict) -> None`

**Purpose**: Extracts draft data from Google Sheets and loads into DuckDB.

**Parameters**:
- `config` (Dict): Configuration dictionary containing sheet credentials and settings

**Google Sheets Requirements**:
- Tab name: "Draft"
- Columns: `round`, `overall_pick`, `name`, `movie`

**Example**:
```python
config = {
    "year": 2025,
    "sheet_name": "2025 Fantasy Box Office Draft",
    "gspread_credentials_name": "GSPREAD_CREDENTIALS_2025"
}
get_draft_data(config)
```

#### `get_movie_data(config: Dict) -> None`

**Purpose**: Extracts movie box office data either from S3 or by scraping Box Office Mojo.

**Data Sources**:
- **S3 Mode**: Reads from `s3://bucket/release_year=YYYY/scraped_date=YYYY-MM-DD/data.parquet`
- **Scrape Mode**: Direct HTTP request to `boxofficemojo.com/year/world/{year}`

**Output Table**: `box_office_mojo_dump` with columns:
- `title`: Movie title
- `revenue`: Worldwide revenue
- `domestic_rev`: Domestic revenue  
- `foreign_rev`: Foreign revenue
- `loaded_date`: Date data was loaded
- `year_part`: Release year

#### `extract(config: Dict) -> None`

**Purpose**: Main extraction function that orchestrates all data extraction.

**Example**:
```python
from dashboard_etl.extract import extract

config = get_config_for_id("my_draft_2025")
extract(config)
```

### `dashboard_etl.transform`

#### `transform(config: Dict) -> None`

**Purpose**: Executes SQL transformation scripts to process raw data.

**Process**:
1. Loads all `*.sql` files from `assets/` directory
2. Replaces `$year` placeholder with actual year
3. Executes scripts in alphabetical order

**SQL Scripts**:
- `1_base_query.sql`: Main data processing and joins
- `2_scoreboard.sql`: Aggregates standings data
- `3_worst_picks.sql`: Calculates missed opportunities

**Example**:
```python
from dashboard_etl.transform import transform

transform(config)
```

### `dashboard_etl.load`

#### `GoogleSheetDashboard` Class

**Purpose**: Manages Google Sheets dashboard creation and formatting.

**Constructor Parameters**:
- `config` (Dict): Configuration containing sheet settings

**Key Attributes**:
- `released_movies_df`: DataFrame with all released movies
- `scoreboard_df`: DataFrame with standings
- `worst_picks_df`: DataFrame with missed opportunities

**Example**:
```python
from dashboard_etl.load import GoogleSheetDashboard

dashboard = GoogleSheetDashboard(config)
```

#### `load(config: Dict) -> None`

**Purpose**: Main loading function that creates and formats the Google Sheets dashboard.

**Dashboard Sections**:
1. **Standings**: Player rankings and stats
2. **Released Movies**: All movies with revenue data
3. **Worst Picks**: Movies with biggest missed opportunities

**Features**:
- Conditional formatting (green for movies still in theaters)
- Auto-resized columns
- Custom formatting for different data types
- Timestamp showing last update

---

## Box Office ETL Module

### `boxofficemojo_etl.etl`

#### `load_worldwide_box_office_to_s3(duckdb_con: DuckDBConnection, year: int, bucket: str) -> int`

**Purpose**: Scrapes Box Office Mojo data for a specific year and uploads to S3.

**Parameters**:
- `duckdb_con`: Database connection with write access
- `year`: Release year to scrape
- `bucket`: S3 bucket name

**Returns**: Number of rows loaded

**S3 Key Format**: `release_year={year}/scraped_date={YYYY-MM-DD}/data.parquet`

#### `extract_worldwide_box_office_data(config: Dict) -> None`

**Purpose**: Main ETL function for Box Office Mojo data extraction.

**Process**:
1. Scrapes current and previous year data
2. Stores in S3 with date partitioning
3. Logs total rows processed

**Example**:
```python
config = {
    "bucket": "my-box-office-bucket",
    "s3_write_access_key_id_var_name": "S3_WRITE_KEY",
    "s3_write_secret_access_key_var_name": "S3_WRITE_SECRET"
}
extract_worldwide_box_office_data(config)
```

---

## Utilities Module

### `utils.read_config`

#### `get_all_ids_from_config() -> List[str]`

**Purpose**: Returns all dashboard IDs from the configuration file.

**Returns**: List of dashboard identifiers

#### `get_config_for_id(id: str) -> Dict`

**Purpose**: Retrieves complete configuration for a specific dashboard ID.

**Parameters**:
- `id` (str): Dashboard identifier

**Returns**: Dictionary with merged dashboard and bucket configuration

**Example**:
```python
from utils.read_config import get_config_for_id

config = get_config_for_id("my_2025_draft")
print(config["name"])  # "2025 Fantasy Box Office Standings"
```

#### `load_override_tables(config: Dict) -> None`

**Purpose**: Loads multiplier and exclusion data from Google Sheets into DuckDB.

**Google Sheets Tabs**:
- **Multipliers and Exclusions**: Movie/round multipliers and exclusions
- **Manual Adds**: Movies to add manually (not in top 200)

### `utils.db_connection`

#### `DuckDBConnection` Class

**Purpose**: Manages DuckDB connections with S3 configuration.

**Constructor Parameters**:
- `config` (Dict): Configuration dictionary
- `need_write_access` (bool): Whether write access to S3 is needed

**Methods**:
- `query(query: str)`: Execute query and return results
- `execute(query: str)`: Execute query without returning results
- `close()`: Close database connection

**Example**:
```python
from utils.db_connection import DuckDBConnection

# Read-only connection
conn = DuckDBConnection(config)
result = conn.query("SELECT COUNT(*) FROM box_office_mojo_dump")
conn.close()

# Write connection for S3 uploads
write_conn = DuckDBConnection(config, need_write_access=True)
```

### `utils.check_config_files`

#### `validate_config(config: Dict) -> bool`

**Purpose**: Validates that all required configuration and environment variables are present.

**Required Config Tags**:
- `name`: Dashboard name
- `description`: Dashboard description  
- `year`: Release year
- `update_type`: Either 'scrape' or 's3'
- `sheet_name`: Google Sheets name

**Required Environment Variables**:
- `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET`
- Google Sheets credentials
- S3 access keys (if using S3 mode)

### `utils.query`

#### `temp_table_to_df(config: Dict, table: str, columns: Optional[List[str]] = None) -> DataFrame`

**Purpose**: Converts a DuckDB table to a pandas DataFrame with optional column renaming.

**Parameters**:
- `config`: Database configuration
- `table`: Table name to query
- `columns`: Optional list of column names for renaming

**Example**:
```python
from utils.query import temp_table_to_df

df = temp_table_to_df(
    config, 
    "base_query",
    columns=["Rank", "Title", "Revenue", "Scored Revenue"]
)
```

### `utils.s3_utils`

#### `load_df_to_s3_table(duckdb_con: DuckDBConnection, df: DataFrame, s3_key: str, bucket_name: str) -> int`

**Purpose**: Uploads a DataFrame to S3 as a Parquet file via DuckDB.

**Process**:
1. Converts DataFrame to temporary JSON file
2. Uses DuckDB to convert JSON to Parquet in S3
3. Cleans up temporary files
4. Returns row count

### `utils.format`

#### `load_format_config(file_path: str) -> dict`

**Purpose**: Loads JSON formatting configuration, removing comment fields.

**Usage**: Used for Google Sheets cell formatting definitions.

### `utils.gspread_format`

#### `df_to_sheet(df: DataFrame, worksheet: Worksheet, location: str, format_dict=None) -> None`

**Purpose**: Uploads DataFrame to Google Sheets with optional formatting.

**Parameters**:
- `df`: DataFrame to upload
- `worksheet`: Google Sheets worksheet object
- `location`: Cell location (e.g., "B4")
- `format_dict`: Optional formatting rules

### `utils.logging_config`

#### `setup_logging() -> None`

**Purpose**: Configures application-wide logging with timestamps and appropriate formatting.

---

## Configuration

### Config File Structure

```yaml
dashboards:
  dashboard_id:
    name: "Dashboard Display Name"
    description: "Dashboard description"
    year: 2025
    update_type: "s3"  # or "scrape"
    sheet_name: "Google Sheets Name"
    gspread_credentials_name: "GSPREAD_CREDENTIALS_2025"  # optional

bucket:
  bucket: "s3-bucket-name"
  s3_read_access_key_id_var_name: "S3_READ_KEY"
  s3_read_secret_access_key_var_name: "S3_READ_SECRET"
  s3_write_access_key_id_var_name: "S3_WRITE_KEY"
  s3_write_secret_access_key_var_name: "S3_WRITE_SECRET"
```

### Environment Variables

```bash
# Modal deployment
MODAL_TOKEN_ID=your_modal_token_id
MODAL_TOKEN_SECRET=your_modal_token_secret

# Google Sheets (JSON format)
GSPREAD_CREDENTIALS_2025='{"type": "service_account", "project_id": "...", ...}'

# S3 credentials
S3_READ_KEY=your_s3_read_access_key
S3_READ_SECRET=your_s3_read_secret_key
S3_WRITE_KEY=your_s3_write_access_key  
S3_WRITE_SECRET=your_s3_write_secret_key
```

---

## Data Models

### Base Query Output

```sql
-- Generated by 1_base_query.sql
CREATE TABLE base_query (
    rank INTEGER,
    title VARCHAR,
    drafted_by VARCHAR,
    revenue INTEGER,
    scored_revenue INTEGER,
    round INTEGER,
    overall_pick INTEGER,
    multiplier DOUBLE,
    domestic_rev INTEGER,
    domestic_pct DOUBLE,
    foreign_rev INTEGER,
    foreign_pct DOUBLE,
    better_pick_title VARCHAR,
    better_pick_scored_revenue INTEGER,
    first_seen_date VARCHAR,
    still_in_theaters VARCHAR
);
```

### Scoreboard Output

```sql
-- Generated by 2_scoreboard.sql
CREATE TABLE scoreboard (
    drafted_by_name VARCHAR,
    scored_revenue INTEGER,
    num_released INTEGER,
    correctly_drafted_pick_count INTEGER,
    correct_pick_pct DOUBLE,
    unadjusted_revenue INTEGER
);
```

---

## Examples

### Basic Usage

```python
from utils.read_config import get_config_for_id
from dashboard_etl.extract import extract
from dashboard_etl.transform import transform
from dashboard_etl.load import load

# Get configuration
config = get_config_for_id("my_2025_draft")

# Run ETL pipeline
extract(config)
transform(config)
load(config)
```

### Custom Data Processing

```python
from utils.db_connection import DuckDBConnection
from utils.query import temp_table_to_df

# Connect to database
conn = DuckDBConnection(config)

# Execute custom query
conn.execute("""
    CREATE TEMP TABLE custom_analysis AS
    SELECT drafted_by, AVG(revenue) as avg_revenue
    FROM base_query
    GROUP BY drafted_by
""")

# Convert to DataFrame
df = temp_table_to_df(config, "custom_analysis")
print(df)

conn.close()
```

### S3 Data Extraction

```python
from boxofficemojo_etl.etl import extract_worldwide_box_office_data

config = {
    "bucket": "my-bucket",
    "s3_write_access_key_id_var_name": "S3_WRITE_KEY",
    "s3_write_secret_access_key_var_name": "S3_WRITE_SECRET"
}

# Extract and upload current year data
extract_worldwide_box_office_data(config)
```

### Manual Dashboard Update

```python
from dashboard_etl.load import GoogleSheetDashboard, update_dashboard

# Create dashboard instance
dashboard = GoogleSheetDashboard(config)

# Update with latest data
update_dashboard(dashboard)
```

---

## Error Handling

The system includes comprehensive error handling:

- **Configuration Validation**: `validate_config()` checks all required settings
- **Missing Movies Logging**: Identifies drafted movies not found in box office data
- **Revenue Validation**: Warns about movies below minimum revenue thresholds
- **Retry Logic**: Modal functions include automatic retries with backoff
- **Connection Management**: Automatic cleanup of database connections

## Performance Considerations

- **S3 Mode**: Recommended for production use, more reliable than direct scraping
- **Parallel Processing**: Multiple dashboards can be processed simultaneously
- **Incremental Updates**: Only processes changed data when possible
- **Efficient Queries**: SQL transformations optimized for large datasets
- **Connection Pooling**: DuckDB connections are reused within ETL cycles

## Security

- **Environment Variables**: Sensitive credentials stored in environment variables
- **Modal Secrets**: Production deployment uses Modal's secret management
- **Access Control**: Separate read/write S3 credentials for principle of least privilege
- **Service Accounts**: Google Sheets access via service account credentials