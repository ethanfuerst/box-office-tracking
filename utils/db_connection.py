import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


class DuckDBConnection:
    def __init__(self, config, id):
        self.connection = duckdb.connect(
            database='box_office_db.duckdb',
            read_only=False,
        )
        self._configure_connection(config, id)

    def _configure_connection(self, config, id):
        s3_access_key_id_var_name = config['dashboards'][id][
            's3_access_key_id_var_name'
        ]
        s3_secret_access_key_var_name = config['dashboards'][id][
            's3_secret_access_key_var_name'
        ]

        s3_access_key_id = os.getenv(s3_access_key_id_var_name)
        s3_secret_access_key = os.getenv(s3_secret_access_key_var_name)

        self.execute(
            f'''
            install httpfs;
            load httpfs;
            CREATE OR REPLACE SECRET (
                TYPE S3,
                KEY_ID '{s3_access_key_id}',
                SECRET '{s3_secret_access_key}',
                REGION 'nyc3',
                ENDPOINT 'nyc3.digitaloceanspaces.com'
            );
            '''
        )

    def query(self, query):
        return self.connection.query(query)

    def execute(self, query):
        self.connection.execute(query)

    def close(self):
        self.connection.close()
