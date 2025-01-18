import glob
from logging import getLogger
from typing import Dict

from utils.db_connection import DuckDBConnection

logger = getLogger(__name__)


def transform(config: Dict) -> None:
    duckdb_con = DuckDBConnection(config)

    for sql_file in sorted(glob.glob('assets/*.sql')):
        with open(sql_file, 'r') as f:
            sql_content = (
                f.read()
                .replace('$year', str(config['year']))
                .replace('$folder_name', str(config['folder_name']))
            )

        duckdb_con.execute(sql_content)

        logger.info(f'Executed {sql_file}')

    duckdb_con.close()
