import logging
from typing import List, Dict
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class TokenDiscovery:
    """
    Token discovery module.
    Makes exactly 1 database query to get all token mints.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def discover_all_token_mints(self) -> List[Dict]:
        """
        Discover ALL token mints from database.
        NO LIMIT - processes full dataset.

        Returns:
            List of dicts with 'mint' key, ready for Polars DataFrame
        """
        query = """
        SELECT DISTINCT mint
        FROM solana.mints
        WHERE mint IS NOT NULL AND mint != ''
        ORDER BY mint
        """

        logger.info('Discovering ALL token mints (no limit)')
        try:
            result = self.db_client.execute_query_dict(query)
            logger.info(f'Discovered {len(result)} total mints')
            return result
        except Exception as e:
            logger.error(f'Failed to discover mints: {e}', exc_info=True)
            return []
