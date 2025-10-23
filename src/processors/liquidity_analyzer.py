import logging
from typing import List, Dict
from ..database import ClickHouseClient

logger = logging.getLogger(__name__)

STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_PRICE_USD = 190.0

class LiquidityAnalyzer:
    """
    Batch-optimized liquidity analyzer.
    Makes exactly 1 database query to get all pool data.
    All processing happens in memory.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_all_pool_metrics_batch(self) -> List[Dict]:
        """
        Get pool metrics for ALL tokens using exactly 1 query.

        Returns:
            List of dicts with pool data ready for Polars DataFrame
            Keys: source, base_coin, quote_coin, base_balance, quote_balance
        """
        logger.info('Fetching pool metrics for all tokens (1 batch query)')

        pool_data = self._get_all_latest_pools()
        logger.info(f'Query 1/1: Retrieved {len(pool_data)} pool records')

        logger.info('Pool metrics fetched successfully')
        return pool_data

    def _get_all_latest_pools(self) -> List[Dict]:
        """
        Single query to get latest pool state for ALL token pairs.
        NO filtering, NO loops.

        Gets latest pool balances grouped by source, base_coin, quote_coin.
        """
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']

        query = f"""
        SELECT
            CASE
                WHEN source LIKE 'jupiter6_%' THEN substring(source, 10)
                WHEN source LIKE 'jupiter4_%' THEN substring(source, 10)
                WHEN source LIKE 'raydium_route_%' THEN substring(source, 15)
                ELSE source
            END AS canonical_source,
            base_coin,
            quote_coin,
            argMax(base_pool_balance_after, block_time) AS last_base_balance,
            argMax(quote_pool_balance_after, block_time) AS last_quote_balance
        FROM solana.swaps
        WHERE
            (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}'))
            OR
            (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}'))
        GROUP BY canonical_source, base_coin, quote_coin
        HAVING last_base_balance > 0 AND last_quote_balance > 0
        """

        logger.info('Executing pool aggregation for ALL tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get pool metrics: {e}', exc_info=True)
            return []

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
