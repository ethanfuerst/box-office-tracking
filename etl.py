import argparse
from logging import getLogger
from typing import List

import modal

from etl_process.extract import extract
from etl_process.load import load
from etl_process.transform import transform
from utils.check_config_files import validate_config
from utils.db_connection import DuckDBConnection
from utils.logging_config import setup_logging
from utils.read_config import get_all_ids_from_config, get_config_for_id

setup_logging()

app = modal.App('box-office-tracking')
logger = getLogger(__name__)

modal_image = modal.Image.debian_slim(python_version='3.10').poetry_install_from_file(
    poetry_pyproject_toml='pyproject.toml'
)

DEFAULT_IDS = get_all_ids_from_config()


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 5 * * *'),
    secrets=[modal.Secret.from_name('box-office-tracking-secrets')],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[modal.Mount.from_local_dir('assets/', remote_path='/root/assets')],
)
def etl(ids: List[str] = DEFAULT_IDS, dry_run: bool = False):
    logger.info(f'Starting ETL process for ids: {", ".join(map(str, ids))}.')

    for id in ids:
        config = get_config_for_id(id=id)

        if not validate_config(config):
            logger.warning(
                f'Config files or env vars for {id} are not configured correctly. Skipping.'
            )
            continue

        duckdb_con = DuckDBConnection(config)

        extract(config=config)
        transform(config=config)
        logger.info(f'All sql scripts have been executed for {id}.')

        if not dry_run:
            logger.info(f'Loading data for {id}.')
            load(config=config)

        logger.info(f'Completed ETL process for {id}.')

        duckdb_con.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run specific functions of the chrono app.'
    )
    parser.add_argument(
        '--ids',
        nargs='+',
        type=str,
        help='IDs to process',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run the E and T steps only, skipping the L to load the data in a google sheet.',
    )
    args = parser.parse_args()

    ids = args.ids if args.ids else DEFAULT_IDS
    dry_run = args.dry_run

    logger.info(
        f'Running ETL locally for ids: {", ".join(map(str, ids))} and dry_run: {dry_run}'
    )
    etl.local(ids=ids, dry_run=dry_run)
    logger.info('ETL process completed.')
