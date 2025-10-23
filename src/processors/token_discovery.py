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
            List of dicts with 'mint' key (as strings), ready for Polars DataFrame
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

            # Decode binary mint addresses to strings and strip null bytes
            decoded_result = []
            for row in result:
                mint_value = row['mint']
                if isinstance(mint_value, bytes):
                    # Decode bytes and strip null bytes
                    mint_str = mint_value.decode('utf-8').rstrip('\x00')
                else:
                    mint_str = str(mint_value).rstrip('\x00')
                decoded_result.append({'mint': mint_str})

            logger.info(f'Discovered {len(decoded_result)} total mints')
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to discover mints: {e}', exc_info=True)
            return []
