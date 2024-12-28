import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from etl_process.extract import extract
from etl_process.transform import transform
from utils.db_connection import DuckDBConnection


def get_duckdb_connection_for_latest_data() -> DuckDBConnection:
    duckdb_con = DuckDBConnection()

    extract()
    transform()

    return duckdb_con
