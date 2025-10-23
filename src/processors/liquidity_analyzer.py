import logging
from typing import List, Dict
import polars as pl
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
    Chunk-optimized liquidity analyzer.
    Makes exactly 1 database query per chunk to get pool data.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_pool_metrics_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Get pool metrics for a SPECIFIC CHUNK of tokens using exactly 1 query.
        Uses WHERE ... IN (...) to filter results for tokens in chunk.

        Args:
            token_addresses: List of token mint addresses to process

        Returns:
            List of dicts with pool data ready for Polars DataFrame
            Keys: canonical_source, base_coin, quote_coin, last_base_balance, last_quote_balance
        """
        if not token_addresses:
            return []

        logger.info(f'Fetching pool metrics for {len(token_addresses)} tokens (1 batch query)')

        pool_data = self._get_pools_for_chunk(token_addresses)
        logger.info(f'Query 1/1: Retrieved {len(pool_data)} pool records')

        logger.info('Pool metrics fetched successfully')
        return pool_data

    def _get_pools_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Query pool data for specific tokens using WHERE IN clause.
        Filters for pools where base_coin OR quote_coin is in the token list.
        """
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']

        # Build WHERE IN clause for token filtering
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])

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
            (
                (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}'))
                OR
                (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}'))
            )
            AND
            (base_coin IN ({placeholders}) OR quote_coin IN ({placeholders}))
        GROUP BY canonical_source, base_coin, quote_coin
        HAVING last_base_balance > 0 AND last_quote_balance > 0
        """

        logger.debug(f'Executing pool aggregation for {len(token_addresses)} tokens')
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
