import argparse
from logging import getLogger

import modal

from etl_process.extract import extract
from etl_process.load import load
from etl_process.transform import transform
from utils.logging_config import setup_logging

setup_logging()

app = modal.App('box-office-tracking')
logger = getLogger(__name__)

modal_image = modal.Image.debian_slim(python_version='3.10').poetry_install_from_file(
    poetry_pyproject_toml='pyproject.toml'
)

DEFAULT_YEARS = [2024]


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
def etl(years: list[int] = DEFAULT_YEARS, dry_run: bool = False):
    logger.info('Starting ETL process.')

    extract()

    for year in years:
        logger.info(f'Transforming data for {year}.')
        transform(year=year)

        if not dry_run:
            logger.info(f'Loading data for {year}.')
            load(year=year)

        logger.info(f'Completed ETL process for {year}.')

    logger.info('ETL process completed.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run specific functions of the chrono app.'
    )
    parser.add_argument(
        '--years',
        nargs='+',
        type=int,
        default=DEFAULT_YEARS,
        help='Years to process',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run the E and T steps only, skipping the L to load the data in a google sheet.',
    )
    args = parser.parse_args()

    years = args.years
    dry_run = args.dry_run

    logger.info(
        f'Running ETL locally with years: {", ".join(map(str, years))} and dry_run: {dry_run}'
    )
    etl.local(years=years, dry_run=dry_run)
    logger.info('ETL process completed.')
