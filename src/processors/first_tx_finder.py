import logging
from datetime import datetime
from typing import Dict, Optional
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class FirstTxFinder:

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def find_first_tx_date(self, token_address: str) -> Optional[datetime]:
        try:
            first_mint = self._get_first_mint_date(token_address)
            first_swap = self._get_first_swap_date(token_address)
            dates = [d for d in [first_mint, first_swap] if d is not None]
            if not dates:
                logger.debug(f'No transactions found for token {token_address[:8]}...')
                return None
            earliest = min(dates)
            logger.debug(f'Token {token_address[:8]}... first tx: {earliest}')
            return earliest
        except Exception as e:
            logger.error(f'Failed to find first tx date for {token_address}: {e}')
            return None

    def find_first_tx_dates_batch(self, token_addresses: list) -> Dict[str, Optional[datetime]]:
        if not token_addresses:
            return {}
        logger.info(f'Finding first tx dates for {len(token_addresses)} tokens (batch mode)')
        first_mints = self._get_first_mints_batch(token_addresses)
        first_swaps = self._get_first_swaps_batch(token_addresses)
        first_dates: Dict[str, Optional[datetime]] = {}
        for token in token_addresses:
            mint_date = first_mints.get(token)
            swap_date = first_swaps.get(token)
            dates = [d for d in [mint_date, swap_date] if d is not None]
            first_dates[token] = min(dates) if dates else None
        logger.info(f'Found first tx dates for {len(first_dates)} tokens')
        return first_dates

    def _get_first_mint_date(self, token_address: str) -> Optional[datetime]:
        query = '\n        SELECT MIN(block_time) as first_mint\n        FROM solana.mints\n        WHERE mint = {token:String}\n        '
        try:
            result = self.db_client.execute_query(query, parameters={'token': token_address})
            if result and result[0][0]:
                return result[0][0]
            return None
        except Exception as e:
            logger.error(f'Failed to get first mint date for {token_address}: {e}')
            return None

    def _get_first_swap_date(self, token_address: str) -> Optional[datetime]:
        query = '\n        SELECT MIN(block_time) as first_swap\n        FROM solana.swaps\n        WHERE base_coin = {token:String} OR quote_coin = {token:String}\n        '
        try:
            result = self.db_client.execute_query(query, parameters={'token': token_address})
            if result and result[0][0]:
                return result[0][0]
            return None
        except Exception as e:
            logger.error(f'Failed to get first swap date for {token_address}: {e}')
            return None

    def _get_first_mints_batch(self, token_addresses: list) -> Dict[str, datetime]:
        if not token_addresses:
            return {}
        normalized = []
        for addr in token_addresses:
            if addr is None:
                continue
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip().replace("'", "''")
            if s:
                normalized.append(f"'{s}'")
        placeholders = ','.join(normalized) if normalized else "''"
        query = f'\n        SELECT mint, MIN(block_time) as first_mint\n        FROM solana.mints\n        WHERE mint IN ({placeholders})\n        GROUP BY mint\n        '
        logger.info('Executing first mint aggregation for provided tokens (%d)', len(token_addresses))
        try:
            result = self.db_client.execute_query(query)
            return {row[0]: row[1] for row in result if row[1]}
        except Exception as e:
            logger.error(f'Failed to get first mints (batch): {e}')
            return {}

    def _get_first_swaps_batch(self, token_addresses: list) -> Dict[str, datetime]:
        if not token_addresses:
            return {}
        normalized = []
        for addr in token_addresses:
            if addr is None:
                continue
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip().replace("'", "''")
            if s:
                normalized.append(f"'{s}'")
        placeholders = ','.join(normalized) if normalized else "''"
        query = f'\n        SELECT\n            token,\n            MIN(block_time) as first_swap\n        FROM (\n            SELECT base_coin as token, block_time\n            FROM solana.swaps\n            WHERE base_coin IN ({placeholders})\n            UNION ALL\n            SELECT quote_coin as token, block_time\n            FROM solana.swaps\n            WHERE quote_coin IN ({placeholders})\n        )\n        GROUP BY token\n        '
        logger.info('Executing first swap aggregation for provided tokens (%d)', len(token_addresses))
        try:
            result = self.db_client.execute_query(query)
            return {row[0]: row[1] for row in result if row[1]}
        except Exception as e:
            logger.error(f'Failed to get first swaps (batch): {e}')
            return {}