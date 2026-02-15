import logging
import os
from pathlib import Path

import duckdb
import s3fs
from pandas import DataFrame

from src import database_name


def get_df_from_s3_parquet(
    s3_path: str,
    bucket_name: str | None = None,
) -> DataFrame:
    '''
    Read DataFrame from S3 Parquet files using DuckDB.

    Args:
        s3_path: S3 path pattern (e.g., 'raw/table_name/partition=value/**/*.parquet')
        bucket_name: S3 bucket name (defaults to S3_BUCKET environment variable)

    Returns:
        DataFrame with the data from S3
    '''
    if not bucket_name:
        bucket_name = os.getenv('S3_BUCKET')

    logging.info(f'Reading DataFrame from s3://{bucket_name}/{s3_path}')

    endpoint = os.getenv('S3_ENDPOINT')
    endpoint_url = (
        f'{endpoint}'
        if not endpoint.startswith('http')
        else endpoint.replace('https://', '').replace('http://', '')
    )

    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute(
        f"""
        CREATE SECRET s3_secret (
            TYPE S3,
            KEY_ID '{os.getenv('S3_ACCESS_KEY_ID')}',
            SECRET '{os.getenv('S3_SECRET_ACCESS_KEY')}',
            REGION '{os.getenv('S3_REGION')}',
            ENDPOINT '{endpoint_url}',
            URL_STYLE 'path',
            USE_SSL true
        );
        """
    )

    query = f"SELECT * FROM read_parquet('s3://{bucket_name}/{s3_path}')"
    df = con.execute(query).df()

    logging.info(f'Read {len(df)} rows from s3://{bucket_name}/{s3_path}')
    return df


def find_latest_partition(
    prefix: str,
    bucket_name: str | None = None,
) -> str | None:
    '''
    Find the latest scraped_date partition under an S3 prefix.

    Args:
        prefix: S3 prefix (e.g., 'raw/release_id_lookup/release_year=2026')
        bucket_name: S3 bucket name (defaults to S3_BUCKET environment variable)

    Returns:
        Relative S3 path to the latest partition, or None if no partitions exist.
    '''
    if not bucket_name:
        bucket_name = os.getenv('S3_BUCKET')

    fs = s3fs.S3FileSystem(
        key=os.getenv('S3_ACCESS_KEY_ID'),
        secret=os.getenv('S3_SECRET_ACCESS_KEY'),
        endpoint_url=f'https://{os.getenv("S3_ENDPOINT")}',
        client_kwargs={'region_name': os.getenv('S3_REGION')},
    )

    try:
        entries = fs.ls(f'{bucket_name}/{prefix}', detail=False)
    except FileNotFoundError:
        return None

    if not entries:
        return None

    # scraped_date=YYYY-MM-DD sorts lexicographically
    latest = sorted(entries)[-1]
    return latest.removeprefix(f'{bucket_name}/')


def load_df_to_s3_parquet(
    df: DataFrame,
    s3_key: str,
    bucket_name: str | None = None,
) -> int:
    '''
    Load DataFrame directly to S3 as Parquet using pandas + s3fs.

    Args:
        df: DataFrame to upload
        s3_key: S3 key path (without .parquet extension)
        bucket_name: S3 bucket name (defaults to S3_BUCKET environment variable)

    Returns:
        Number of rows loaded
    '''
    if not bucket_name:
        bucket_name = os.getenv('S3_BUCKET')

    logging.info(f'Loading DataFrame to s3://{bucket_name}/{s3_key}.parquet')

    access_key_id = os.getenv('S3_ACCESS_KEY_ID')
    secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY')
    endpoint = os.getenv('S3_ENDPOINT')
    region = os.getenv('S3_REGION')

    fs = s3fs.S3FileSystem(
        key=access_key_id,
        secret=secret_access_key,
        endpoint_url=f'https://{endpoint}',
        client_kwargs={'region_name': region},
    )

    s3_file = f'{bucket_name}/{s3_key}.parquet'

    with fs.open(s3_file, 'wb') as f:
        df.to_parquet(f, engine='pyarrow', index=False)

    rows_loaded = len(df)
    logging.info(
        f'Updated s3://{bucket_name}/{s3_key}.parquet with {rows_loaded} rows.'
    )

    return rows_loaded


def load_duckdb_table_to_s3_parquet(
    database_path: Path | str,
    table_name: str,
    s3_key: str,
    schema_name: str,
    bucket_name: str | None = None,
) -> int:
    '''
    Load DuckDB table to S3 as Parquet by querying to DataFrame first.

    Args:
        database_path: Path to the DuckDB database file
        table_name: Name of the table in DuckDB
        s3_key: S3 key path (without .parquet extension)
        schema_name: Schema name (e.g., 'published')
        bucket_name: S3 bucket name (defaults to S3_BUCKET environment variable)

    Returns:
        Number of rows loaded
    '''
    if not bucket_name:
        bucket_name = os.getenv('S3_BUCKET')

    logging.info(
        f'Loading DuckDB table {schema_name}.{table_name} to s3://{bucket_name}/{s3_key}.parquet'
    )

    database_path_str = str(database_path)

    with duckdb.connect(database=database_path_str) as con:
        df = con.query(f'select * from {database_name}.{schema_name}.{table_name}').df()

    return load_df_to_s3_parquet(df=df, s3_key=s3_key, bucket_name=bucket_name)
