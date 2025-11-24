from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml


@dataclass
class S3SyncConfig:
    """Configuration for S3 sync operations."""

    bucket: str
    s3_read_access_key_id_var_name: str = 'S3_ACCESS_KEY_ID'
    s3_read_secret_access_key_var_name: str = 'S3_SECRET_ACCESS_KEY'
    s3_write_access_key_id_var_name: str = ''
    s3_write_secret_access_key_var_name: str = ''

    def __post_init__(self):
        """Validate required fields."""
        if not self.bucket:
            raise ValueError('bucket is required')
        if not self.s3_write_access_key_id_var_name:
            raise ValueError('s3_write_access_key_id_var_name is required')
        if not self.s3_write_secret_access_key_var_name:
            raise ValueError('s3_write_secret_access_key_var_name is required')

    @classmethod
    def from_yaml(cls, config_path: Path | str) -> 'S3SyncConfig':
        """Load configuration from a YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f'Config file not found: {config_path}')

        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)

        return cls(**config_dict)

    def get_s3_access_key_id_var_name(
        self, access_type: Literal['read', 'write'] = 'read'
    ) -> str:
        """Get the environment variable name for S3 access key ID."""
        if access_type == 'write':
            return self.s3_write_access_key_id_var_name
        return self.s3_read_access_key_id_var_name

    def get_s3_secret_access_key_var_name(
        self, access_type: Literal['read', 'write'] = 'read'
    ) -> str:
        """Get the environment variable name for S3 secret access key."""
        if access_type == 'write':
            return self.s3_write_secret_access_key_var_name
        return self.s3_read_secret_access_key_var_name
