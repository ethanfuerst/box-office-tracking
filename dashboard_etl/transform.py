import glob
import logging
from typing import Dict

from utils.logging_config import setup_logging

setup_logging()
from utils.db_connection import DuckDBConnection


def transform(config: Dict) -> None:
    duckdb_con = DuckDBConnection(config)

    for sql_file in sorted(glob.glob('assets/*.sql')):
        with open(sql_file, 'r') as f:
            sql_content = f.read().replace('$year', str(config['year']))

        duckdb_con.execute(sql_content)

        logging.info(f'Executed {sql_file}')

    duckdb_con.close()
