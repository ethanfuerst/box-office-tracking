import logging

from src import database_path
from src.utils.logging_config import setup_logging
from src.utils.s3_utils import load_duckdb_table_to_s3_parquet

setup_logging()


def main() -> None:
    logging.info(
        'Reading published.worldwide_box_office from DuckDB database for publishing.'
    )

    load_duckdb_table_to_s3_parquet(
        database_path=database_path,
        table_name='worldwide_box_office',
        s3_key='published_tables/daily_ranks/v1/data',
        schema_name='published',
    )
