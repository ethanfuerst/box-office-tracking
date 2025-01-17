import os
from logging import getLogger
from typing import Dict

import lxml
import pandas as pd

from utils.logging_config import setup_logging

setup_logging()

logger = getLogger(__name__)

CONFIG_FILES = [
    'assets/drafts/{folder_name}/box_office_draft.csv',
    'assets/drafts/{folder_name}/manual_adds.csv',
]


def config_files_exist(config: Dict) -> bool:
    REQUIRED_ENV_VARS = [
        'MODAL_TOKEN_ID',
        'MODAL_TOKEN_SECRET',
        config['gspread_credentials_name'],
        config.get('s3_access_key_id_var_name', 'S3_ACCESS_KEY_ID'),
        config.get('s3_secret_access_key_var_name', 'S3_SECRET_ACCESS_KEY'),
    ]

    for var in REQUIRED_ENV_VARS:
        env_var = var.format(folder_name=config['folder_name'])

        if os.getenv(env_var) is None:
            logger.warning(f'{env_var} is not set in the .env file.')
            return False

    for config_file in CONFIG_FILES:
        config_file_path = config_file.format(folder_name=config['folder_name'])

        if not os.path.exists(config_file_path):
            logger.warning(f'{config_file_path} does not exist.')
            return False

        if 'box_office_draft.csv' in config_file:
            box_office_draft_df = pd.read_csv(config_file_path)
            if box_office_draft_df.empty:
                logger.warning(f'{config_file_path} does not contain any rows.')
                return False

    return True
