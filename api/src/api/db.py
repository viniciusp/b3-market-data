"""Connection pool for the serving store."""

import os

from psycopg_pool import ConnectionPool


def create_pool() -> ConnectionPool:
    dsn = os.environ.get("DATABASE_URL", "postgresql://app:app@postgres:5432/b3")
    return ConnectionPool(conninfo=dsn, min_size=1, max_size=4, open=False)
