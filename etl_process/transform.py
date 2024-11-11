import glob
import logging
import pandas as pd
from utils.db_connection import DuckDBConnection
from utils.query import temp_table_to_df


logger = logging.getLogger(__name__)


def transform(year: int) -> None:
    duckdb_con = DuckDBConnection()
    for sql_file in sorted(glob.glob("assets/*.sql")):
        with open(sql_file, "r") as f:
            sql_content = f.read()

        if sql_file == "assets/1_base_query.sql":
            duckdb_con.execute(sql_content, [year])
        else:
            duckdb_con.execute(sql_content)
        logger.info(f"Executed {sql_file}")
    logger.info("All sql scripts have been executed.")
