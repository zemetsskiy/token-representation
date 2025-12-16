from .db import ClickHouseClient, get_db_client, get_solana_db_client, get_evm_db_client

# Postgres is optional for ClickHouse-only workflows (e.g., dry runs).
# Import lazily so environments without psycopg2 can still use ClickHouse tooling.
try:
    from .postgres import PostgresClient, get_postgres_client  # type: ignore
except Exception as _pg_import_err:  # pragma: no cover
    PostgresClient = None  # type: ignore

    def get_postgres_client():  # type: ignore
        raise ImportError(
            "Postgres client is unavailable (likely missing 'psycopg2'). "
            "Install deps from token_representation/requirements.txt or use the project venv."
        ) from _pg_import_err

# Redis is optional for EVM price calculation (falls back to CH-only pricing).
try:
    from .redis_client import RedisClient, get_redis_client, RedisPriceNotFoundError  # type: ignore
except Exception as _redis_import_err:  # pragma: no cover
    RedisClient = None  # type: ignore
    RedisPriceNotFoundError = Exception  # type: ignore

    def get_redis_client():  # type: ignore
        raise ImportError(
            "Redis client is unavailable (likely missing 'redis'). "
            "Install deps from token_representation/requirements.txt or use the project venv."
        ) from _redis_import_err

__all__ = [
    'ClickHouseClient',
    'get_db_client',
    'get_solana_db_client',
    'get_evm_db_client',
    'PostgresClient',
    'get_postgres_client',
    'RedisClient',
    'get_redis_client',
    'RedisPriceNotFoundError',
]


