import datetime
import logging
import os
import ssl

from pandas import read_html
from sqlmesh.core.context import Context

from src import project_root
from src.utils.s3_utils import load_df_to_s3_parquet, load_duckdb_table_to_s3_parquet

S3_DATE_FORMAT = '%Y-%m-%d'


ssl._create_default_https_context = ssl._create_unverified_context


def load_worldwide_box_office_to_s3(year: int) -> int:
    logging.info(f'Starting extraction for {year}.')

    try:
        df = read_html(f'https://www.boxofficemojo.com/year/world/{year}')[0]
    except Exception as e:
        logging.error(f'Failed to fetch data: {e}')
        return 0

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)

    s3_key = f'release_year={year}/scraped_date={formatted_date}/data'

    rows_loaded = load_df_to_s3_parquet(df=df, s3_key=s3_key)

    return rows_loaded


def extract_worldwide_box_office() -> None:
    logging.info('Extracting worldwide box office data.')

    current_year = datetime.date.today().year
    last_year = current_year - 1

    logging.info(f'Running for {current_year} and {last_year}')

    total_rows_loaded = 0
    bucket = os.getenv('S3_BUCKET')

    for year in [current_year, last_year]:
        total_rows_loaded += load_worldwide_box_office_to_s3(year=year)

    logging.info(f'Total rows loaded to {bucket}: {total_rows_loaded}')


def extract() -> None:
    extract_worldwide_box_office()


def transform() -> None:
    logging.info('Running SQLMesh plan and apply.')
    sqlmesh_context = Context(paths=project_root / 'src' / 'sqlmesh_project')

    plan = sqlmesh_context.plan()
    sqlmesh_context.apply(plan)
    _ = sqlmesh_context.run()


def load() -> None:
    logging.info('Connecting to SQLMesh database for publishing.')
    sqlmesh_context = Context(paths=project_root / 'src' / 'sqlmesh_project')

    engine_adapter = sqlmesh_context.engine_adapter
    duckdb_con = engine_adapter.connection

    load_duckdb_table_to_s3_parquet(
        duckdb_con=duckdb_con,
        table_name='worldwide_box_office',
        s3_key='published_tables/daily_ranks/v1/data',
        schema_name='published',
    )
