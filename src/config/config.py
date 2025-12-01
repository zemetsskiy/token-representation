import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    # ClickHouse Configuration (Data Source)
    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
    CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'solana')
    CLICKHOUSE_TEMP_DATABASE = os.getenv('CLICKHOUSE_TEMP_DATABASE', 'temp_processing')

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
    SOL_PRICE_KEY = os.getenv('SOL_PRICE_KEY', 'solana:price_usd')

    # Constants
    STABLECOINS = {'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'}
    SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
    SOL_PRICE_USD = 188
    
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