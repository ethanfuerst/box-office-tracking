import argparse

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
def run_pipeline(force_all_extracts: bool = False, skip_extracts: bool = False):
    if not skip_extracts:
        extract(force_all=force_all_extracts)
    transform()
    load()


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--force-all-extracts',
        action='store_true',
        help='Force all extract functions to run regardless of day',
    )
    parser.add_argument(
        '--skip-extracts',
        action='store_true',
        help='Skip the extract step entirely',
    )
    args = parser.parse_args()

    force_all_extracts = args.force_all_extracts
    skip_extracts = args.skip_extracts

    if force_all_extracts and skip_extracts:
        parser.error('--force-all-extracts and --skip-extracts are mutually exclusive')

    run_pipeline.local(
        force_all_extracts=force_all_extracts,
        skip_extracts=skip_extracts,
    )
