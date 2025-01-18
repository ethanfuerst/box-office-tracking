import datetime
import logging
import os
import ssl
from typing import Dict

from pandas import DataFrame, read_html

from utils.db_connection import DuckDBConnection
from utils.logging_config import setup_logging
from utils.s3_utils import load_df_to_s3_table

S3_DATE_FORMAT = '%Y%m%d'
setup_logging()


ssl._create_default_https_context = ssl._create_unverified_context


def load_worldwide_box_office_to_s3(
    duckdb_con: DuckDBConnection, year: int, bucket: str
) -> int:
    logging.info(f'Starting extraction for {year}.')

    try:
        df = read_html(f'https://www.boxofficemojo.com/year/world/{year}')[0]
    except Exception as e:
        logging.error(f'Failed to fetch data: {e}')
        return 0

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)

    s3_key = f'boxofficemojo_{year}_{formatted_date}'

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=df,
        s3_key=s3_key,
        bucket_name=bucket,
    )

    return rows_loaded


def extract_worldwide_box_office_data(bucket: str) -> None:
    logging.info('Extracting worldwide box office data.')

    duckdb_con = DuckDBConnection(
        config={
            's3_access_key_id_var_name': 'BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID',
            's3_secret_access_key_var_name': 'BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY',
        }
    ).connection

    current_year = datetime.date.today().year
    last_year = current_year - 1

    logging.info(f'Running for {current_year} and {last_year}')

    total_rows_loaded = 0

    for year in [current_year, last_year]:
        total_rows_loaded += load_worldwide_box_office_to_s3(
            duckdb_con=duckdb_con, year=year, bucket=bucket
        )

    logging.info(f'Total rows loaded to {bucket}: {total_rows_loaded}')
