import argparse
import logging
from typing import List

import modal
from modal.runner import deploy_app

from boxofficemojo_etl.etl import extract_worldwide_box_office_data
from dashboard_etl.extract import extract
from dashboard_etl.load import load
from dashboard_etl.transform import transform
from utils.check_config_files import validate_config
from utils.db_connection import DuckDBConnection
from utils.logging_config import setup_logging
from utils.read_config import get_all_ids_from_config, get_config_for_id

setup_logging()

app = modal.App('box-office-tracking')

modal_image = modal.Image.debian_slim(python_version='3.10').poetry_install_from_file(
    poetry_pyproject_toml='pyproject.toml'
)

DEFAULT_IDS = get_all_ids_from_config()
CONFIG_MOUNT = modal.Mount.from_local_dir('config/', remote_path='/root/config')


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 4 * * *'),
    secrets=[modal.Secret.from_name('box-office-tracking-secrets')],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[CONFIG_MOUNT],
)
def s3_sync(ids: List[str] = DEFAULT_IDS):
    all_configs = [get_config_for_id(id=id) for id in ids]
    ids_with_s3_update_type = [
        id for id in ids if all_configs[ids.index(id)].get('update_type') == 's3'
    ]
    if not ids_with_s3_update_type:
        logging.info('No ids with s3 update type found. Skipping S3 sync.')
        return

    logging.info(
        f'Syncing S3 bucket for ids: {", ".join(map(str, ids_with_s3_update_type))}.'
    )
    buckets_to_sync = {
        (all_configs[ids_with_s3_update_type.index(id)]['bucket'])
        for id in ids_with_s3_update_type
    }

    logging.info(f'Syncing data to buckets: {", ".join(map(str, buckets_to_sync))}.')
    for config in all_configs:
        if config['update_type'] == 's3' and config['bucket'] in buckets_to_sync:
            extract_worldwide_box_office_data(config=config)
            buckets_to_sync.remove(config['bucket'])

    logging.info(
        f'S3 bucket synced for ids: {", ".join(map(str, ids_with_s3_update_type))}.'
    )


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 5 * * *'),
    secrets=[modal.Secret.from_name('box-office-tracking-secrets')],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[CONFIG_MOUNT],
)
def update_dashboards(ids: List[str] = DEFAULT_IDS, dry_run: bool = False):
    logging.info(f'Starting ETL process for ids: {", ".join(map(str, ids))}.')

    for id in ids:
        config = get_config_for_id(id=id)

        if not validate_config(config):
            logging.warning(
                f'Config files or env vars for {id} are not configured correctly. Skipping.'
            )
            continue

        duckdb_con = DuckDBConnection(config)

        extract(config=config)
        transform(config=config)
        logging.info(f'All sql scripts have been executed for {id}.')

        if not dry_run:
            logging.info(f'Loading data for {id}.')
            load(config=config)

        logging.info(f'Completed ETL process for {id}.')

        duckdb_con.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run the ETL process with specific parameters.'
    )
    parser.add_argument(
        '--ids',
        nargs='+',
        type=str,
        help='IDs to process. If not provided, all ids in the config.yml will be processed.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Skips loading the data in a google sheet.',
    )
    parser.add_argument(
        '--sync-s3',
        action='store_true',
        help='Run the S3 sync process.',
    )
    args = parser.parse_args()

    ids = args.ids if args.ids else DEFAULT_IDS
    dry_run = args.dry_run
    sync_s3 = args.sync_s3

    if sync_s3:
        logging.info('Running S3 sync locally.')
        s3_sync.local(ids=ids)
        logging.info('S3 sync completed.')

    logging.info(
        f'Updating dashboards locally for ids: {", ".join(map(str, ids))} and dry_run: {dry_run}'
    )
    update_dashboards.local(ids=ids, dry_run=dry_run)
    logging.info('Dashboard update process completed.')
