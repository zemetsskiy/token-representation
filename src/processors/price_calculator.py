import logging
from typing import List, Dict
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
    Batch-optimized price calculator.
    Makes exactly 1 database query to get all latest prices.
    All processing happens in memory.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_all_prices_batch(self) -> List[Dict]:
        """
        Get latest prices for ALL tokens using exactly 1 query.

        Returns:
            List of dicts with price data ready for Polars DataFrame
            Keys: token, last_price_in_sol
        """
        logger.info('Fetching prices for all tokens (1 batch query)')

        price_data = self._get_all_latest_prices()
        logger.info(f'Query 1/1: Retrieved {len(price_data)} price records')

        logger.info('Prices fetched successfully')
        return price_data

    def _get_all_latest_prices(self) -> List[Dict]:
        """
        Single aggregate query for ALL tokens' latest prices vs SOL.
        NO filtering, NO loops.

        Gets the latest price from swaps where one side is SOL.
        """
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
            WHERE quote_coin = '{SOL_ADDRESS}'

            UNION ALL

            -- token is quote vs SOL
            SELECT
                quote_coin AS token,
                block_time,
                base_coin_amount / NULLIF(quote_coin_amount, 0) AS price
            FROM solana.swaps
            WHERE base_coin = '{SOL_ADDRESS}'
        )
        GROUP BY token
        """

        logger.info('Executing price aggregation for ALL tokens')
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
