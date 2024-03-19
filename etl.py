import modal
import logging
import argparse
from etl.extract import extract
from etl.transform import transform
from etl.load import load
from utils.logging_config import setup_logging

setup_logging()

stub = modal.Stub("box-office-tracking")
logger = logging.getLogger(__name__)

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install duckdb==0.10.0",
    "pip install gspread-formatting==1.1.2",
    "pip install gspread==6.0.2",
    "pip install html5lib==1.1",
    "pip install lxml==5.1.0",
    "pip install pandas==2.1.4",
    "pip install requests==2.31.0",
    "pip install python-dotenv==1.0.1",
)

parser = argparse.ArgumentParser(
    description="Run the application locally or deploy to Modal."
)
parser.add_argument(
    "--deploy", action="store_true", help="Deploy the application to Modal."
)
args = parser.parse_args()


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 4 * * *"),
    secrets=[modal.Secret.from_name("box-office-tracking-secrets")],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[modal.Mount.from_local_dir("assets/", remote_path="/root/assets")],
)
def etl():
    logger.info("Starting ETL process.")
    extract()
    transform()
    load()
    logger.info("Completed ETL process.")


if __name__ == "__main__":
    if args.deploy:
        logger.info("Deploying etl to Modal.")
        modal.runner.deploy_stub(stub)
    else:
        logger.info("Running ETL locally.")
        etl.local()
