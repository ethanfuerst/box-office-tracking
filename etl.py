from logging import getLogger

import modal

from etl_process.extract import extract
from etl_process.load import load
from etl_process.transform import transform
from utils.logging_config import setup_logging

setup_logging()

app = modal.App("box-office-tracking")
logger = getLogger(__name__)

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


@app.function(
    image=modal_image,
    schedule=modal.Cron("0 5 * * *"),
    secrets=[modal.Secret.from_name("box-office-tracking-secrets")],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
    mounts=[modal.Mount.from_local_dir("assets/", remote_path="/root/assets")],
)
def etl(years: list[int] = [2024]):
    logger.info("Starting ETL process.")

    extract()

    for year in years:
        transform(year=year)
        load(year=year)

        logger.info(f"Completed ETL process for {year}.")

    logger.info("ETL process completed.")


if __name__ == "__main__":
    etl.local()
