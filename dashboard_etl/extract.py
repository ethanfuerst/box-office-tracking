import logging
import os
import ssl
from typing import Dict

import duckdb
from pandas import read_html

from utils.logging_config import setup_logging

setup_logging()

from utils.db_connection import DuckDBConnection

S3_DATE_FORMAT = '%Y%m%d'


def extract(config: Dict) -> None:
    duckdb_con = DuckDBConnection(config)

    if config['update_type'] == 's3':
        duckdb_con.execute(
            f'''
            create or replace table box_office_mojo_dump as (
                with all_data as (
                    select
                        *
                        , split_part(split_part(filename, '_', -1), '.', -2) as date_str
                        , split_part(filename, '_', -2) as year_part_from_s3
                    from read_parquet('s3://{config['bucket']}/boxofficemojo_*.parquet', filename=true)
                )

                select
                    "Release Group" as title
                    , coalesce(try_cast(replace("Worldwide"[2:], ',', '') as integer), 0) as revenue
                    , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                    , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                    , strptime(date_str, '{S3_DATE_FORMAT}') as loaded_date
                    , if(
                        year_part_from_s3 = 'ytd'
                        , try_cast(date_part('year', loaded_date) as int)
                        , try_cast(year_part_from_s3 as int)
                    ) as year_part
                from all_data
            )
            '''
        )

        row_count = 'select count(*) from box_office_mojo_dump'
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
            duckdb_con.execute("CREATE OR REPLACE TABLE all_data AS SELECT * FROM df")

            logging.info('DataFrame loaded into DuckDB table all_data.')

            duckdb_con.execute(
                f'''
                create or replace table box_office_mojo_dump as (
                    select
                        "Release Group" as title
                        , coalesce(try_cast(replace("Worldwide"[2:], ',', '') as integer), 0) as revenue
                        , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                        , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                        , current_date as loaded_date
                        , {year} as year_part
                    from all_data
                )
                '''
            )
            logging.info('DataFrame loaded into DuckDB table box_office_mojo_dump.')

        except Exception as e:
            logging.error(f'Failed to fetch data: {e}')

    duckdb_con.close()
