import psycopg
from psycopg.rows import dict_row


DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pipeline_db",
    "user": "pipeline_user",
    "password": "pipeline_pass",
}


def get_db_connection():
    """
    Create and return a new database connection.
    """
    return psycopg.connect(
        **DATABASE_CONFIG,
        row_factory=dict_row
    )
