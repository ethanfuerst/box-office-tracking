import glob
import logging
import pandas as pd
from utils.db_connection import DuckDBConnection
from utils.query import query_to_str, temp_table_to_df


logger = logging.getLogger(__name__)


def transform() -> None:
    duckdb_con = DuckDBConnection()
    for sql_file in sorted(glob.glob("assets/*.sql")):
        duckdb_con.execute(query_to_str(sql_file))
        logger.info(f"Executed {sql_file}")
    logger.info("All sql scripts have been executed.")
