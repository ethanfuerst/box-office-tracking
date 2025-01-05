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
            """
            install httpfs;
            load httpfs;
            """
        )
        conn.execute(
            f"""
            set s3_endpoint='nyc3.digitaloceanspaces.com';
            set s3_region='nyc3';
            set s3_access_key_id='{s3_access_key_id}';
            set s3_secret_access_key='{s3_secret_access_key}';
            """
        )

    @classmethod
    def close(cls):
        if cls._instance is not None:
            cls._instance.connection.close()
            cls._instance = None
