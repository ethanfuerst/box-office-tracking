import modal

from src import project_root
from src.etl import s3_sync

app = modal.App('box-office-tracking')

modal_image = (
    modal.Image.debian_slim(python_version='3.12')
    .pip_install_from_pyproject('pyproject.toml')
    .add_local_file(
        'src/duckdb_databases/.gitkeep',
        remote_path='/root/src/duckdb_databases/.gitkeep',
        copy=True,
    )
    .add_local_dir('src/config/', remote_path='/root/src/config')
    .add_local_python_source('src')
)


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 7 * * *'),
    secrets=[modal.Secret.from_name('box-office-tracking-secrets')],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def run_s3_sync():
    s3_sync(config_path=project_root / 'src/config/s3_sync.yml')


if __name__ == '__main__':
    run_s3_sync.local()
