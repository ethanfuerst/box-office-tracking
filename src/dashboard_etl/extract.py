import json
import logging
import os
import ssl
from typing import Dict

from gspread import service_account_from_dict
from pandas import DataFrame, read_html

from src.utils.logging_config import setup_logging

setup_logging()

from src.utils.db_connection import DuckDBConnection

S3_DATE_FORMAT = '%Y-%m-%d'


def get_draft_data(config: Dict) -> None:
    gspread_credentials_name = config.get(
        'gspread_credentials_name', f'GSPREAD_CREDENTIALS_{config["year"]}'
    )

    credentials_dict = json.loads(
        os.getenv(gspread_credentials_name).replace('\n', '\\n')
    )
    gc = service_account_from_dict(credentials_dict)
    worksheet = gc.open(config['sheet_name']).worksheet('Draft')

    raw = worksheet.get_all_values()
    df = DataFrame(data=raw[1:], columns=raw[0]).astype(str)

    duckdb_con = DuckDBConnection(config)

    duckdb_con.connection.register('df', df)
    duckdb_con.execute('create or replace table raw_drafter as select * from df')

    duckdb_con.close()


def get_movie_data(config: Dict) -> None:
    duckdb_con = DuckDBConnection(config)

    if config['update_type'] == 's3':
        duckdb_con.execute(
            f'''
            create or replace table raw_box_office_mojo_dump as (
                with all_data as (
                    select
                        *
                        , split_part(split_part(filename, 'release_year=', 2), '/', 1) as year_part_from_s3
                        , strptime(split_part(split_part(filename, 'scraped_date=', 2), '/', 1), '{S3_DATE_FORMAT}') as scraped_date_from_s3
                    from read_parquet('s3://{config['bucket']}/release_year=*/scraped_date=*/data.parquet', filename=true)
                )

                select
                    "Release Group" as title
                    , coalesce(try_cast(replace("Worldwide"[2:], ',', '') as integer), 0) as revenue
                    , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                    , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                    , scraped_date_from_s3 as loaded_date
                    , year_part_from_s3 as year_part
                from all_data
            )
            '''
        )

        row_count = 'select count(*) from raw_box_office_mojo_dump'
        logging.info(
            f'Read {duckdb_con.query(row_count).fetchnumpy()["count_star()"][0]} rows with query from s3 bucket'
        )

    else:
        logging.info('Skipping extract step and getting data from Box Office Mojo.')
        try:
            year = config['year']

            ssl._create_default_https_context = ssl._create_unverified_context

            df = read_html(f'https://www.boxofficemojo.com/year/world/{year}')[0]

            logging.info(f'Read {len(df)} rows with scrape from Box Office Mojo')

            duckdb_con.connection.register('df', df)
            duckdb_con.execute(
                "CREATE OR REPLACE TABLE raw_all_data AS SELECT * FROM df"
            )

            logging.info('DataFrame loaded into DuckDB table raw_all_data.')

            duckdb_con.execute(
                f'''
                create or replace table raw_box_office_mojo_dump as (
                    select
                        "Release Group" as title
                        , coalesce(try_cast(replace("Worldwide"[2:], ',', '') as integer), 0) as revenue
                        , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                        , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                        , current_date as loaded_date
                        , {year} as year_part
                    from raw_all_data
                )
                '''
            )
            logging.info('DataFrame loaded into DuckDB table raw_box_office_mojo_dump.')

        except Exception as e:
            logging.error(f'Failed to fetch data: {e}')

    duckdb_con.close()


def extract(config: Dict) -> None:
    get_movie_data(config)
    get_draft_data(config)
