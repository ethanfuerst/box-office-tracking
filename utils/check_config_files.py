import os
from logging import getLogger

from utils.logging_config import setup_logging

setup_logging()

logger = getLogger(__name__)

CONFIG_FILES = [
    'assets/drafts/{year}/box_office_draft.csv',
    'assets/drafts/{year}/manual_adds.csv',
]
REQUIRED_ENV_VARS = [
    'S3_ACCESS_KEY_ID',
    'S3_SECRET_ACCESS_KEY',
    'GSPREAD_CREDENTIALS_{year}',
    'MODAL_TOKEN_ID',
    'MODAL_TOKEN_SECRET',
]


def config_files_exist(year: int) -> bool:
    for var in REQUIRED_ENV_VARS:
        env_var = var.format(year=year)

        if os.getenv(env_var) is None:
            logger.warning(f'{env_var} is not set in the .env file.')
            return False

    for config_file in CONFIG_FILES:
        config_file_path = config_file.format(year=year)

        if not os.path.exists(config_file_path):
            logger.warning(f'{config_file_path} does not exist.')
            return False

    return True
