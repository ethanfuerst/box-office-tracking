import os

import duckdb
from dotenv import load_dotenv

from src import project_root

load_dotenv()


class DuckDBConnection:
    def __init__(self, config, need_write_access=False):
        database_name = (
            project_root / 'src' / 'duckdb_databases' / config.get('database_file')
        )

        self.connection = duckdb.connect(
            database=str(database_name),
            read_only=False,
        )
        self.need_write_access = need_write_access
        self._configure_connection(config)

    def _configure_connection(self, config):
        access_type = 'write' if self.need_write_access else 'read'
        s3_access_key_id_var_name = config.get(
            f's3_{access_type}_access_key_id_var_name',
            'S3_ACCESS_KEY_ID',
        )
        s3_secret_access_key_var_name = config.get(
            f's3_{access_type}_secret_access_key_var_name',
            'S3_SECRET_ACCESS_KEY',
        )

        self.connection.execute(
            f'''
            install httpfs;
            load httpfs;
            CREATE OR REPLACE SECRET {access_type}_secret (
                TYPE S3,
                KEY_ID '{os.getenv(s3_access_key_id_var_name)}',
                SECRET '{os.getenv(s3_secret_access_key_var_name)}',
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

    def df(self, query):
        return self.connection.query(query).df()
