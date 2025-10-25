import logging
from typing import Dict

from sqlmesh.core.context import Context

from src import project_root
from src.utils.logging_config import setup_logging

setup_logging()


def transform(config: Dict) -> None:
    # Set environment variables for SQLMesh config
    import os

    os.environ['DASHBOARD_ID'] = config.get('dashboard_id', 'default')
    os.environ['YEAR'] = str(config.get('year', 2025))
    os.environ['GSPREAD_CREDENTIALS_NAME'] = config.get(
        'gspread_credentials_name', f'GSPREAD_CREDENTIALS_{config.get("year", 2025)}'
    )
    os.environ['GSPREAD_SHEET_NAME'] = config.get('sheet_name')
    os.environ['UPDATE_TYPE'] = config.get('update_type', 's3')
    os.environ['BUCKET'] = config.get('bucket', '')
    os.environ['S3_READ_ACCESS_KEY_NAME'] = config.get(
        's3_read_access_key_id_var_name', 'S3_READ_ACCESS_KEY_ID'
    )
    os.environ['S3_READ_SECRET_ACCESS_KEY_NAME'] = config.get(
        's3_read_secret_access_key_var_name', 'S3_READ_SECRET_ACCESS_KEY'
    )

    sqlmesh_context = Context(
        paths=project_root / 'src' / 'dashboard_etl' / 'sqlmesh_project'
    )

    plan = sqlmesh_context.plan()
    sqlmesh_context.apply(plan)
    output = sqlmesh_context.run()

    logging.info(output)
