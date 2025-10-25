import os

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

from src import project_root

dashboard_id = os.getenv('DASHBOARD_ID', 'default')
year = os.getenv('YEAR', '2025')

config = Config(
    model_defaults=ModelDefaultsConfig(dialect='duckdb'),
    gateways={
        'duckdb': GatewayConfig(
            connection=DuckDBConnectionConfig(
                database=str(project_root / f'box_office_db_{dashboard_id}.duckdb'),
                extensions=[
                    {'name': 'httpfs'},
                ],
                secrets=[
                    {
                        'type': 'S3',
                        'region': 'nyc3',
                        'endpoint': 'nyc3.digitaloceanspaces.com',
                        'key_id': os.getenv(os.getenv('S3_READ_ACCESS_KEY_NAME')),
                        'secret': os.getenv(
                            os.getenv('S3_READ_SECRET_ACCESS_KEY_NAME')
                        ),
                    },
                ],
            )
        )
    },
    variables={
        'dashboard_id': dashboard_id,
        'year': year,
        'update_type': os.getenv('UPDATE_TYPE', 's3'),
        'bucket': os.getenv('BUCKET', ''),
    },
)
