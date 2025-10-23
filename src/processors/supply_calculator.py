import logging
from typing import List, Dict
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class SupplyCalculator:
    """
    Batch-optimized supply calculator.
    Makes exactly 2 database queries total (minted + burned aggregates).
    All processing happens in memory.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def get_all_supplies_batch(self) -> List[Dict]:
        """
        Get supply data for ALL tokens in database using exactly 2 queries.

        Returns:
            List of dicts with keys: mint, total_minted, total_burned
            Ready to be converted to Polars DataFrame
        """
        logger.info('Fetching supply data for all tokens (2 batch queries)')

        # Query 1: Get all minted amounts
        minted_data = self._get_all_minted()
        logger.info(f'Query 1/2: Retrieved {len(minted_data)} minted records')

        # Query 2: Get all burned amounts
        burned_data = self._get_all_burned()
        logger.info(f'Query 2/2: Retrieved {len(burned_data)} burned records')

        # Merge in memory (will be done in Polars in main.py)
        # But provide raw data for flexibility
        logger.info('Supply data fetched successfully')

        return {
            'minted': minted_data,
            'burned': burned_data
        }

    def _get_all_minted(self) -> List[Dict]:
        """
        Single aggregate query for ALL tokens' minted amounts.
        NO filtering, NO loops.
        """
        query = """
        SELECT
            mint,
            SUM(amount) AS total_minted
        FROM solana.mints
        GROUP BY mint
        """

        logger.info('Executing minted aggregation for ALL tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get minted amounts: {e}', exc_info=True)
            return []

    def _get_all_burned(self) -> List[Dict]:
        """
        Single aggregate query for ALL tokens' burned amounts.
        NO filtering, NO loops.
        """
        query = """
        SELECT
            mint,
            SUM(amount) AS total_burned
        FROM solana.burns
        GROUP BY mint
        """

        logger.info('Executing burned aggregation for ALL tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get burned amounts: {e}', exc_info=True)
            return []
