import datetime
import logging
import ssl

import duckdb
from pandas import read_html
from sqlmesh.core.context import Context

from src import project_root
from src.utils.config import S3SyncConfig
from src.utils.db_connection import DuckDBConnection
from src.utils.s3_utils import load_df_to_s3_table

S3_DATE_FORMAT = '%Y-%m-%d'


ssl._create_default_https_context = ssl._create_unverified_context


def load_worldwide_box_office_to_s3(
    duckdb_wrapper: DuckDBConnection,
    year: int,
    bucket: str,
) -> int:
    logging.info(f'Starting extraction for {year}.')

    try:
        df = read_html(f'https://www.boxofficemojo.com/year/world/{year}')[0]
    except Exception as e:
        logging.error(f'Failed to fetch data: {e}')
        return 0

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)

    s3_key = f'release_year={year}/scraped_date={formatted_date}/data'

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_wrapper.connection,
        df=df,
        s3_key=s3_key,
        bucket_name=bucket,
    )

    return rows_loaded


def publish_tables_to_s3_from_connection(
    duckdb_con: duckdb.DuckDBPyConnection, bucket: str
) -> int:
    """Publish tables to S3 using a DuckDB connection directly."""
    logging.info('Publishing tables to S3.')

    df = duckdb_con.execute(
        '''
        SELECT
            title,
            revenue,
            domestic_rev,
            foreign_rev,
            loaded_date,
            release_year,
            published_timestamp_utc
        FROM "box_office_tracking_sqlmesh_db"."published"."worldwide_box_office"
        '''
    ).df()

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=df,
        s3_key='published_tables/daily_ranks/v1/data',
        bucket_name=bucket,
    )

    return rows_loaded


def parse_config(config_path: str) -> S3SyncConfig:
    return S3SyncConfig.from_yaml(config_path)


def extract(config_path: str) -> None:
    logging.info('Extracting worldwide box office data.')
    config = parse_config(config_path)

    with DuckDBConnection(config=config) as duckdb_wrapper:
        current_year = datetime.date.today().year
        last_year = current_year - 1

        logging.info(f'Running for {current_year} and {last_year}')

        total_rows_loaded = 0
        bucket = config.bucket

        for year in [current_year, last_year]:
            total_rows_loaded += load_worldwide_box_office_to_s3(
                duckdb_wrapper=duckdb_wrapper, year=year, bucket=bucket
            )

        logging.info(f'Total rows loaded to {bucket}: {total_rows_loaded}')


def transform(config_path: str) -> None:
    logging.info('Running SQLMesh plan and apply.')
    sqlmesh_context = Context(paths=project_root / 'src' / 'sqlmesh_project')

    plan = sqlmesh_context.plan()
    sqlmesh_context.apply(plan)
    _ = sqlmesh_context.run()


def load(config_path: str) -> None:
    config = parse_config(config_path)
    bucket = config.bucket

    logging.info('Connecting to SQLMesh database for publishing.')
    sqlmesh_context = Context(paths=project_root / 'src' / 'sqlmesh_project')

    engine_adapter = sqlmesh_context.engine_adapter
    duckdb_con = engine_adapter.connection

    publish_tables_to_s3_from_connection(duckdb_con=duckdb_con, bucket=bucket)
