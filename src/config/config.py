import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
    CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'solana')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    SOLANA_HTTP_RPC_URL = os.getenv('SOLANA_HTTP_RPC_URL')
    METAPLEX_PROGRAM_ID = os.getenv('METAPLEX_PROGRAM_ID', 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s')
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