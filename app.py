import argparse
import datetime

import modal
from dotenv import load_dotenv

from src.etl import extract, load, transform

app = modal.App('box-office-tracking')

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


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 7 * * *'),
    secrets=[modal.Secret.from_name('box-office-tracking-secrets')],
    timeout=60 * 20,
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def run_pipeline(
    extract_names: list[str] | None = None,
    years: list[int] | None = None,
):
    if years is None:
        current_year = datetime.date.today().year
        years = [current_year, current_year - 1]
    extract_errors = extract(extract_names=extract_names, years=years)
    transform()
    load()

    if extract_errors:
        failed = ', '.join(name for name, _ in extract_errors)
        raise RuntimeError(f'Extraction failed for: {failed}')


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--extracts',
        nargs='+',
        help=(
            'Extract(s) to run by name (e.g. --extracts release_domestic worldwide_box_office). '
            'Use --extracts all to force all extracts regardless of schedule.'
        ),
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=None,
        help='First year to extract (inclusive). Defaults to current_year - 1.',
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=None,
        help='Last year to extract (inclusive). Defaults to current_year.',
    )
    args = parser.parse_args()

    current_year = datetime.date.today().year
    start = args.start_year if args.start_year is not None else current_year - 1
    end = args.end_year if args.end_year is not None else current_year
    if start > end:
        parser.error(f'--start-year ({start}) must be <= --end-year ({end}).')
    years = list(range(start, end + 1))

    run_pipeline.local(extract_names=args.extracts, years=years)
