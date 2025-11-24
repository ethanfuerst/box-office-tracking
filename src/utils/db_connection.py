import os

import duckdb
from dotenv import load_dotenv

from src import project_root
from src.utils.config import S3SyncConfig

load_dotenv()


class DuckDBConnection:
    def __init__(self, config: S3SyncConfig, need_write_access=False):
        database_name = (
            project_root / 'src' / 'duckdb_databases' / 'box_office_tracking_db.duckdb'
        )

        self.connection = duckdb.connect(
            database=str(database_name),
            read_only=False,
        )
        self.need_write_access = need_write_access
        self._configure_connection(config)

    def _configure_connection(self, config: S3SyncConfig):
        access_type = 'write' if self.need_write_access else 'read'
        s3_access_key_id_var_name = config.get_s3_access_key_id_var_name(access_type)
        s3_secret_access_key_var_name = config.get_s3_secret_access_key_var_name(
            access_type
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
