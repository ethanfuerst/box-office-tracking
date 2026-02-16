"""Automated daily backfill of historical Box Office Mojo data.

Deploys as a separate Modal app that runs daily. Each run:
1. Discovers which years (1977 to current) are missing data in S3
2. Picks the most recent missing year
3. Runs all 5 extracts for that year (in dependency order)
4. Self-completes when all years have data

Deploy:
    uv run modal deploy backfill.py

Monitor:
    Check Modal dashboard for 'box-office-tracking-backfill' app.

Run locally:
    uv run python backfill.py              # auto-discover next missing year
    uv run python backfill.py --year 1990  # backfill a specific year
"""

import argparse
import datetime
import logging

import modal
from dotenv import load_dotenv

from src.etl import extract
from src.utils.logging_config import setup_logging
from src.utils.s3_utils import list_year_partitions

setup_logging()

EARLIEST_YEAR = 1977

# Extracts partitioned by release_year (independent, run first)
INDEPENDENT_EXTRACTS = [
    'worldwide_box_office',
    'release_id_lookup',
]

# Extracts that depend on release_id_lookup S3 data
DEPENDENT_EXTRACTS = [
    'release_domestic',
    'release_metadata',
    'release_worldwide_snapshot',
]

app = modal.App('box-office-tracking-backfill')

modal_image = (
    modal.Image.debian_slim(python_version='3.12')
    .pip_install_from_pyproject('pyproject.toml')
    .add_local_file(
        'src/duckdb_databases/.gitkeep',
        remote_path='/root/src/duckdb_databases/.gitkeep',
        copy=True,
    )
    .add_local_dir(
        'src/sqlmesh_project/',
        remote_path='/root/src/sqlmesh_project',
    )
    .add_local_python_source('src')
)


def find_missing_years() -> list[int]:
    """Return years (descending) that are missing from S3.

    A year is considered 'present' when both worldwide_box_office and
    release_id_lookup have data for it.
    """
    current_year = datetime.date.today().year
    all_years = set(range(EARLIEST_YEAR, current_year + 1))

    covered = None
    for extract_name in INDEPENDENT_EXTRACTS:
        year_set = list_year_partitions(extract_name)
        if covered is None:
            covered = year_set
        else:
            covered = covered & year_set

    if covered is None:
        covered = set()

    missing = sorted(all_years - covered, reverse=True)
    return missing


@app.function(
    image=modal_image,
    schedule=modal.Cron('30 8 * * *'),
    secrets=[modal.Secret.from_name('box-office-tracking-secrets')],
    timeout=60 * 40,
    retries=modal.Retries(
        max_retries=2,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def run_backfill(year_override: int | None = None):
    """Process one missing year of backfill data.

    Args:
        year_override: If provided, process this specific year instead
            of auto-discovering.
    """
    if year_override is not None:
        target_year = year_override
        logging.info(f'Backfill: processing override year {target_year}.')
    else:
        missing = find_missing_years()
        if not missing:
            logging.info('Backfill complete: all years 1977-present have data.')
            return
        target_year = missing[0]
        logging.info(
            f'Backfill: {len(missing)} years remaining. '
            f'Processing year {target_year}.'
        )

    years = [target_year]

    # Phase 1: independent extracts (worldwide_box_office, release_id_lookup)
    logging.info(f'Backfill phase 1: independent extracts for {target_year}.')
    phase1_errors = extract(
        extract_names=INDEPENDENT_EXTRACTS,
        years=years,
    )

    if phase1_errors:
        failed = ', '.join(name for name, _ in phase1_errors)
        raise RuntimeError(f'Backfill phase 1 failed for year {target_year}: {failed}')

    # Phase 2: dependent extracts (need release_id_lookup data from phase 1)
    logging.info(f'Backfill phase 2: dependent extracts for {target_year}.')
    phase2_errors = extract(
        extract_names=DEPENDENT_EXTRACTS,
        years=years,
    )

    if phase2_errors:
        failed = ', '.join(name for name, _ in phase2_errors)
        raise RuntimeError(f'Backfill phase 2 failed for year {target_year}: {failed}')

    logging.info(f'Backfill: year {target_year} complete.')


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser(description='Run backfill locally.')
    parser.add_argument(
        '--year',
        type=int,
        default=None,
        help='Specific year to backfill. If omitted, auto-discovers next missing year.',
    )
    args = parser.parse_args()

    run_backfill.local(year_override=args.year)
