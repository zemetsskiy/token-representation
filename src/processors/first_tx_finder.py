import logging
from typing import List, Dict
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class FirstTxFinder:
    """
    Batch-optimized first transaction finder.
    Makes exactly 2 database queries total (first mints + first swaps).
    All processing happens in memory.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def get_all_first_tx_dates_batch(self) -> Dict:
        """
        Get first transaction dates for ALL tokens using exactly 2 queries.

        Returns:
            Dict with keys: 'first_mints', 'first_swaps'
            Each containing list of dicts ready for Polars DataFrame
        """
        logger.info('Fetching first tx dates for all tokens (2 batch queries)')

        # Query 1: Get first mint date for all tokens
        first_mints = self._get_all_first_mints()
        logger.info(f'Query 1/2: Retrieved {len(first_mints)} first mint records')

        # Query 2: Get first swap date for all tokens
        first_swaps = self._get_all_first_swaps()
        logger.info(f'Query 2/2: Retrieved {len(first_swaps)} first swap records')

        logger.info('First tx dates fetched successfully')

        return {
            'first_mints': first_mints,
            'first_swaps': first_swaps
        }

    def _get_all_first_mints(self) -> List[Dict]:
        """
        Single aggregate query for ALL tokens' first mint dates.
        NO filtering, NO loops.
        """
        query = """
        SELECT
            mint,
            MIN(block_time) as first_mint
        FROM solana.mints
        GROUP BY mint
        """

        logger.info('Executing first mint aggregation for ALL tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get first mints: {e}', exc_info=True)
            return []

    def _get_all_first_swaps(self) -> List[Dict]:
        """
        Single aggregate query for ALL tokens' first swap dates.
        Uses UNION ALL to check both base_coin and quote_coin.
        NO filtering, NO loops.
        """
        query = """
        SELECT
            token,
            MIN(block_time) as first_swap
        FROM (
            SELECT base_coin as token, block_time
            FROM solana.swaps

            UNION ALL

            SELECT quote_coin as token, block_time
            FROM solana.swaps
        )
        GROUP BY token
        """

        logger.info('Executing first swap aggregation for ALL tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get first swaps: {e}', exc_info=True)
            return []
