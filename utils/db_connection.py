import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


class DuckDBConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckDBConnection, cls).__new__(cls)
            cls._instance.connection = duckdb.connect(
                database=":memory:", read_only=False
            )
            cls._configure_connection(cls._instance.connection)
        return cls._instance.connection

    @staticmethod
    def _configure_connection(conn):
        s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
        s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")

        conn.execute(
            f"""
            install httpfs;
            load httpfs;
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{s3_access_key_id}',
                SECRET '{s3_secret_access_key}',
                REGION 'nyc3',
                ENDPOINT 'nyc3.digitaloceanspaces.com'
            );
            """
        )

    @classmethod
    def close(cls):
        if cls._instance is not None:
            cls._instance.connection.close()
            cls._instance = None
