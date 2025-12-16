import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    # ClickHouse Configuration - Solana (Data Source)
    # Falls back to generic CLICKHOUSE_* if SOLANA_CLICKHOUSE_* not set
    SOLANA_CLICKHOUSE_HOST = os.getenv('SOLANA_CLICKHOUSE_HOST', os.getenv('CLICKHOUSE_HOST', 'localhost'))
    SOLANA_CLICKHOUSE_PORT = int(os.getenv('SOLANA_CLICKHOUSE_PORT', os.getenv('CLICKHOUSE_PORT', '8123')))
    SOLANA_CLICKHOUSE_USER = os.getenv('SOLANA_CLICKHOUSE_USER', os.getenv('CLICKHOUSE_USER', 'default'))
    SOLANA_CLICKHOUSE_PASSWORD = os.getenv('SOLANA_CLICKHOUSE_PASSWORD', os.getenv('CLICKHOUSE_PASSWORD', ''))
    SOLANA_CLICKHOUSE_DATABASE = os.getenv('SOLANA_CLICKHOUSE_DATABASE', os.getenv('CLICKHOUSE_DATABASE', 'solana'))
    SOLANA_CLICKHOUSE_TEMP_DATABASE = os.getenv('SOLANA_CLICKHOUSE_TEMP_DATABASE', os.getenv('CLICKHOUSE_TEMP_DATABASE', 'temp_processing'))

    # ClickHouse Configuration - EVM (Data Source)
    # Falls back to generic CLICKHOUSE_* if EVM_CLICKHOUSE_* not set
    EVM_CLICKHOUSE_HOST = os.getenv('EVM_CLICKHOUSE_HOST', os.getenv('CLICKHOUSE_HOST', 'localhost'))
    EVM_CLICKHOUSE_PORT = int(os.getenv('EVM_CLICKHOUSE_PORT', os.getenv('CLICKHOUSE_PORT', '8123')))
    EVM_CLICKHOUSE_USER = os.getenv('EVM_CLICKHOUSE_USER', os.getenv('CLICKHOUSE_USER', 'default'))
    EVM_CLICKHOUSE_PASSWORD = os.getenv('EVM_CLICKHOUSE_PASSWORD', os.getenv('CLICKHOUSE_PASSWORD', ''))
    EVM_CLICKHOUSE_DATABASE = os.getenv('EVM_CLICKHOUSE_DATABASE', os.getenv('CLICKHOUSE_DATABASE', 'evm'))
    EVM_CLICKHOUSE_TEMP_DATABASE = os.getenv('EVM_CLICKHOUSE_TEMP_DATABASE', os.getenv('CLICKHOUSE_TEMP_DATABASE', 'temp_processing'))

    # Legacy/backwards compatibility aliases (used by shared code)
    CLICKHOUSE_HOST = SOLANA_CLICKHOUSE_HOST
    CLICKHOUSE_PORT = SOLANA_CLICKHOUSE_PORT
    CLICKHOUSE_USER = SOLANA_CLICKHOUSE_USER
    CLICKHOUSE_PASSWORD = SOLANA_CLICKHOUSE_PASSWORD
    CLICKHOUSE_DATABASE = SOLANA_CLICKHOUSE_DATABASE
    CLICKHOUSE_TEMP_DATABASE = SOLANA_CLICKHOUSE_TEMP_DATABASE

    # PostgreSQL Configuration (Storage)
    # Use connection string if provided, otherwise fall back to individual parameters
    POSTGRES_CONNECTION_STRING = os.getenv('POSTGRES_CONNECTION_STRING', None)
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'token_metrics')

    # Worker Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '1000000'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # Solana Configuration
    SOLANA_HTTP_RPC_URL = os.getenv('SOLANA_HTTP_RPC_URL')
    METAPLEX_PROGRAM_ID = os.getenv('METAPLEX_PROGRAM_ID', 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s')

    # Redis Configuration (Price Feeder)
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '2'))  # Default to DB 2 as per user request
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

    # Native token price keys in Redis
    # Keys must match exactly what's stored in Redis
    SOL_PRICE_KEY = os.getenv('SOL_PRICE_KEY', 'solana:price_usd')
    ETH_PRICE_KEY = os.getenv('ETH_PRICE_KEY', 'ethereum:price_usd')
    BNB_PRICE_KEY = os.getenv('BNB_PRICE_KEY', 'bnb:price_usd')
    MATIC_PRICE_KEY = os.getenv('MATIC_PRICE_KEY', 'matic:price_usd')

    # Chain to native price key mapping
    NATIVE_PRICE_KEYS = {
        'solana': SOL_PRICE_KEY,
        'eth': ETH_PRICE_KEY,
        'base': ETH_PRICE_KEY,  # Base uses ETH as native
        'bsc': BNB_PRICE_KEY,
        'polygon': MATIC_PRICE_KEY,
    }

    # Constants
    STABLECOINS = {'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'}
    SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
    # SOL_PRICE_USD removed - must be fetched from Redis at runtime
    
    @classmethod
    def validate(cls):
        required_fields = ['CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER', 'CLICKHOUSE_DATABASE']
        missing = []
        for field in required_fields:
            if not getattr(cls, field):
                missing.append(field)
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        return True
Config.validate()