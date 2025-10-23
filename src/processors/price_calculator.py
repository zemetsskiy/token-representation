import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
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

    def get_prices_for_chunk(self, token_addresses: List[str]) -> pl.DataFrame:
        """
        Get latest prices for a SPECIFIC CHUNK of tokens using exactly 1 query.
        Uses WHERE ... IN (...) to filter results.

        Args:
            token_addresses: List of token mint addresses to process

        Returns:
            Polars DataFrame with columns: mint, price_in_sol, price_usd
        """
        if not token_addresses:
            return pl.DataFrame({'mint': [], 'price_in_sol': [], 'price_usd': []})

        logger.info(f'Fetching prices for {len(token_addresses)} tokens (1 batch query)')

        price_data = self._get_prices_for_chunk(token_addresses)
        logger.info(f'Query 1/1: Retrieved {len(price_data)} price records')

        # Convert to Polars DataFrame
        df_prices = pl.DataFrame(price_data) if price_data else pl.DataFrame({'token': [], 'last_price_in_sol': []})

        # Rename token column to mint for consistency
        if len(df_prices) > 0:
            df_prices = df_prices.rename({'token': 'mint', 'last_price_in_sol': 'price_in_sol'})

        # Create base DataFrame with all tokens in chunk
        df_chunk = pl.DataFrame({'mint': token_addresses})

        # Join price data
        df_chunk = df_chunk.join(df_prices, on='mint', how='left')

        # Fill nulls with 0 and calculate USD price
        df_chunk = df_chunk.with_columns([
            pl.col('price_in_sol').fill_null(0),
            (pl.col('price_in_sol').fill_null(0) * SOL_PRICE_USD).alias('price_usd')
        ])

        logger.info(f'Prices fetched and processed for {len(df_chunk)} tokens')
        return df_chunk

    def _get_prices_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Query latest prices for specific tokens using WHERE IN clause.
        """
        # Build WHERE IN clause
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])

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
            WHERE quote_coin = '{SOL_ADDRESS}' AND base_coin IN ({placeholders})

            UNION ALL

            -- token is quote vs SOL
            SELECT
                quote_coin AS token,
                block_time,
                base_coin_amount / NULLIF(quote_coin_amount, 0) AS price
            FROM solana.swaps
            WHERE base_coin = '{SOL_ADDRESS}' AND quote_coin IN ({placeholders})
        )
        GROUP BY token
        """

        logger.debug(f'Executing price aggregation for {len(token_addresses)} tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
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
