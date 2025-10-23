import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
from ..config import Config
logger = logging.getLogger(__name__)

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_PRICE_USD = 190.0
STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}

class PriceCalculator:
    """
    Chunk-optimized price calculator.
    Makes exactly 1 database query per chunk to get latest prices.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_prices_for_chunk(self) -> pl.DataFrame:
        """
        Get latest prices for tokens in chunk_tokens temporary table using exactly 1 query.
        Uses WHERE IN (SELECT mint FROM chunk_tokens) to filter results.

        Returns:
            Polars DataFrame with columns: mint, price_in_sol, price_usd
        """
        logger.info('Fetching prices from chunk_tokens table (1 batch query)')

        price_data = self._get_prices_for_chunk()
        logger.info(f'Query 1/1: Retrieved {len(price_data)} price records')

        # Convert to Polars DataFrame with explicit schema
        if price_data:
            df_prices = pl.DataFrame(
                price_data,
                schema={'token': pl.Utf8, 'last_price_in_sol': pl.Float64}
            )
        else:
            df_prices = pl.DataFrame(
                {'token': [], 'last_price_in_sol': []},
                schema={'token': pl.Utf8, 'last_price_in_sol': pl.Float64}
            )

        # Rename token column to mint for consistency
        if len(df_prices) > 0:
            df_prices = df_prices.rename({'token': 'mint', 'last_price_in_sol': 'price_in_sol'})

        # Fill nulls with 0 and calculate USD price
        df_prices = df_prices.with_columns([
            pl.col('price_in_sol').fill_null(0),
            (pl.col('price_in_sol').fill_null(0) * SOL_PRICE_USD).alias('price_usd')
        ])

        logger.info(f'Prices fetched and processed for {len(df_prices)} tokens')
        return df_prices

    def _get_prices_for_chunk(self) -> List[Dict]:
        """
        Query latest prices for tokens in temp database chunk_tokens table.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        query = f"""
        SELECT
            token,
            argMax(price, block_time) AS last_price_in_sol
        FROM (
            -- token is base vs SOL
            SELECT
                base_coin AS token,
                block_time,
                quote_coin_amount / NULLIF(base_coin_amount, 0) AS price
            FROM solana.swaps
            WHERE quote_coin = '{SOL_ADDRESS}' AND base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)

            UNION ALL

            -- token is quote vs SOL
            SELECT
                quote_coin AS token,
                block_time,
                base_coin_amount / NULLIF(quote_coin_amount, 0) AS price
            FROM solana.swaps
            WHERE base_coin = '{SOL_ADDRESS}' AND quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
        )
        GROUP BY token
        """

        logger.debug(f'Executing price aggregation from {temp_db}.chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)
            # Decode binary token addresses to strings
            decoded_result = []
            for row in result:
                token_value = row['token']
                if isinstance(token_value, bytes):
                    token_str = token_value.decode('utf-8').rstrip('\x00')
                else:
                    token_str = str(token_value).rstrip('\x00')
                decoded_result.append({'token': token_str, 'last_price_in_sol': row['last_price_in_sol']})
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get prices: {e}', exc_info=True)
            return []

    def get_sol_price(self) -> float:
        """Return current SOL price in USD."""
        return self.sol_price_usd

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
