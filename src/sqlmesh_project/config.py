import datetime
import os
from datetime import timezone

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

from src import project_root
from src.utils.config import S3SyncConfig

S3_REGION = 'nyc3'
S3_ENDPOINT = 'nyc3.digitaloceanspaces.com'
S3_SECRET_TYPE = 'S3'
S3_ACCESS_KEY_ID_VAR_NAME = 'BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID'
S3_SECRET_ACCESS_KEY_VAR_NAME = 'BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY'

config_from_yaml = S3SyncConfig.from_yaml(project_root / 'src/config/s3_sync.yml')

config = Config(
    model_defaults=ModelDefaultsConfig(dialect='duckdb'),
    gateways={
        'duckdb': GatewayConfig(
            connection=DuckDBConnectionConfig(
                database=str(
                    project_root
                    / 'src'
                    / 'duckdb_databases'
                    / 'box_office_tracking_sqlmesh_db.duckdb'
                ),
                extensions=[
                    {'name': 'httpfs'},
                ],
                secrets={
                    'write_secret': {
                        'type': S3_SECRET_TYPE,
                        'region': S3_REGION,
                        'endpoint': S3_ENDPOINT,
                        'key_id': os.getenv(S3_ACCESS_KEY_ID_VAR_NAME),
                        'secret': os.getenv(S3_SECRET_ACCESS_KEY_VAR_NAME),
                    }
                },
            )
        )
    },
    variables={
        'bucket': config_from_yaml.bucket,
        'year': datetime.datetime.now(timezone.utc).year,
    },
)
