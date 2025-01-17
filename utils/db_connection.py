import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


class DuckDBConnection:
    def __init__(self, config):
        self.connection = duckdb.connect(
            database='box_office_db.duckdb',
            read_only=False,
        )
        self._configure_connection(config)

    def _configure_connection(self, config):
        s3_access_key_id_var_name = config.get(
            's3_access_key_id_var_name', 'S3_ACCESS_KEY_ID'
        )
        s3_secret_access_key_var_name = config.get(
            's3_secret_access_key_var_name', 'S3_SECRET_ACCESS_KEY'
        )

        s3_access_key_id = os.getenv(s3_access_key_id_var_name)
        s3_secret_access_key = os.getenv(s3_secret_access_key_var_name)

        self.connection.execute(
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

    def execute(self, query, *args, **kwargs):
        self.connection.execute(query, *args, **kwargs)

    def close(self):
        self.connection.close()
