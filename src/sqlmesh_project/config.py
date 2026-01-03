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
                        'type': 'S3',
                        'region': os.getenv('S3_REGION'),
                        'endpoint': os.getenv('S3_ENDPOINT'),
                        'key_id': os.getenv('S3_ACCESS_KEY_ID'),
                        'secret': os.getenv('S3_SECRET_ACCESS_KEY'),
                    }
                },
            )
        )
    },
    variables={
        'bucket': os.getenv('S3_BUCKET'),
        'year': datetime.datetime.now(timezone.utc).year,
    },
)
