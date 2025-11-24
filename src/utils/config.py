from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class S3SyncConfig:
    """Configuration for S3 sync operations."""

    bucket: str
    s3_access_key_id_var_name: str = ''
    s3_secret_access_key_var_name: str = ''

    def __post_init__(self):
        """Validate required fields."""
        if not self.bucket:
            raise ValueError('bucket is required')
        if not self.s3_access_key_id_var_name:
            raise ValueError('s3_access_key_id_var_name is required')
        if not self.s3_secret_access_key_var_name:
            raise ValueError('s3_secret_access_key_var_name is required')

    @classmethod
    def from_yaml(cls, config_path: Path | str) -> 'S3SyncConfig':
        """Load configuration from a YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f'Config file not found: {config_path}')

        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)

        return cls(**config_dict)
