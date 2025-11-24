import datetime
import logging
import ssl

from pandas import read_html

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


def publish_tables_to_s3(duckdb_wrapper: DuckDBConnection, bucket: str) -> None:
    logging.info('Publishing tables to S3.')

    df = duckdb_wrapper.query(
        f'''
            with all_data as (
                select
                    *
                    , split_part(split_part(filename, 'release_year=', 2), '/', 1) as year_part_from_s3
                    , strptime(split_part(split_part(filename, 'scraped_date=', 2), '/', 1), '%Y-%m-%d') as scraped_date_from_s3
                from read_parquet('s3://{bucket}/release_year=*/scraped_date=*/data.parquet', filename=true)
            )
            select
                "Release Group" as title
                , coalesce(try_cast(replace(substring("Worldwide", 2), ',', '') as integer), 0) as revenue
                , coalesce(try_cast(replace(substring("Domestic", 2), ',', '') as integer), 0) as domestic_rev
                , coalesce(try_cast(replace(substring("Foreign", 2), ',', '') as integer), 0) as foreign_rev
                , cast(scraped_date_from_s3 as date) as loaded_date
                , cast(year_part_from_s3 as int) as year_part
                , now() as published_timestamp_utc
            from all_data
        '''
    ).df()

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_wrapper.connection,
        df=df,
        s3_key='published_tables/daily_ranks/data',
        bucket_name=bucket,
    )

    return rows_loaded


def s3_sync(config_path: str) -> None:
    logging.info('Extracting worldwide box office data.')
    config = S3SyncConfig.from_yaml(config_path)

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

        publish_tables_to_s3(duckdb_wrapper=duckdb_wrapper, bucket=bucket)
