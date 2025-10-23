import logging
from typing import List, Set
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class TokenDiscovery:

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def discover_token_mints(self) -> List[str]:
        query = "\n        SELECT DISTINCT mint\n        FROM solana.mints\n        WHERE mint IS NOT NULL AND mint != ''\n        ORDER BY mint\n        LIMIT 100\n        "
        try:
            result = self.db_client.execute_query(query)
            mints = [row[0] for row in result if row and row[0]]
            logger.info(f'Discovered {len(mints)} mints')
            return mints
        except Exception as e:
            logger.error(f'Failed to discover mints: {e}')
            return []