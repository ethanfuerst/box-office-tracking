import logging
import os
from typing import Dict

import lxml

from src.utils.logging_config import setup_logging

setup_logging()


def validate_config(config: Dict) -> bool:
    REQUIRED_ENV_VARS = [
        'MODAL_TOKEN_ID',
        'MODAL_TOKEN_SECRET',
        config['gspread_credentials_name'],
        config.get('s3_read_access_key_id_var_name', 'S3_ACCESS_KEY_ID'),
        config.get('s3_read_secret_access_key_var_name', 'S3_SECRET_ACCESS_KEY'),
        config.get('s3_write_access_key_id_var_name', 'S3_ACCESS_KEY_ID'),
        config.get('s3_write_secret_access_key_var_name', 'S3_SECRET_ACCESS_KEY'),
    ]
    REQUIRED_TAGS = [
        'name',
        'description',
        'year',
        'update_type',
        'sheet_name',
    ]

    for tag in REQUIRED_TAGS:
        if tag not in config:
            logging.warning(f'Missing required config tag: {tag}')
            return False

    for var in REQUIRED_ENV_VARS:
        if os.getenv(var) is None:
            logging.warning(f'{var} is not set in the .env file.')
            return False

    return True
