import os
from logging import getLogger
from typing import Dict

import pandas as pd

from utils.logging_config import setup_logging

setup_logging()

logger = getLogger(__name__)

CONFIG_FILES = [
    'assets/drafts/{id}/box_office_draft.csv',
    'assets/drafts/{id}/manual_adds.csv',
]


def config_files_exist(config: Dict, id: str) -> bool:
    REQUIRED_ENV_VARS = [
        'MODAL_TOKEN_ID',
        'MODAL_TOKEN_SECRET',
        'BUCKET',
        config['dashboards'][id]['gspread_credentials_name'],
        config['dashboards'][id]['s3_access_key_id_var_name'],
        config['dashboards'][id]['s3_secret_access_key_var_name'],
    ]

    for var in REQUIRED_ENV_VARS:
        env_var = var.format(id=id)

        if os.getenv(env_var) is None:
            logger.warning(f'{env_var} is not set in the .env file.')
            return False

    for config_file in CONFIG_FILES:
        config_file_path = config_file.format(id=id)

        if not os.path.exists(config_file_path):
            logger.warning(f'{config_file_path} does not exist.')
            return False

        if 'box_office_draft.csv' in config_file:
            box_office_draft_df = pd.read_csv(config_file_path)
            if box_office_draft_df.empty:
                logger.warning(f'{config_file_path} does not contain any rows.')
                return False

    return True
