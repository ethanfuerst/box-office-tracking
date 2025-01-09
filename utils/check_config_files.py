import os
from logging import getLogger

import pandas as pd

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
    'BUCKET',
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

    box_office_draft_path = f'assets/drafts/{year}/box_office_draft.csv'

    box_office_draft_df = pd.read_csv(box_office_draft_path)
    if box_office_draft_df.empty:
        logger.warning(f'{box_office_draft_path} does not contain any rows.')
        return False

    return True
