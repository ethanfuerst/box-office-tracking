import logging
import os

import duckdb
import s3fs
from pandas import DataFrame

from src.utils.logging_config import setup_logging

setup_logging()


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
    duckdb_con: duckdb.DuckDBPyConnection,
    table_name: str,
    s3_key: str,
    schema_name: str,
    bucket_name: str | None = None,
) -> int:
    '''
    Load DuckDB table to S3 as Parquet by querying to DataFrame first.

    Args:
        duckdb_con: DuckDB connection
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
        f'Loading DuckDB table {table_name} to s3://{bucket_name}/{s3_key}.parquet'
    )

    if schema_name:
        qualified_table = f'"{schema_name}"."{table_name}"'
    else:
        qualified_table = f'"{table_name}"'

    df = duckdb_con.execute(f'select * from {qualified_table}').df()

    return load_df_to_s3_parquet(df=df, s3_key=s3_key, bucket_name=bucket_name)
