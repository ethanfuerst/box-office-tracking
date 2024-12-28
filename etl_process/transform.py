import glob
from logging import getLogger

from utils.db_connection import DuckDBConnection

logger = getLogger(__name__)


def transform(year: int) -> None:
    duckdb_con = DuckDBConnection()

    for sql_file in sorted(glob.glob("assets/*.sql")):
        with open(sql_file, "r") as f:
            sql_content = f.read().replace("$year", str(year))

        duckdb_con.execute(sql_content)

        logger.info(f"Executed {sql_file}")
    logger.info(f"All sql scripts have been executed for {year}.")
