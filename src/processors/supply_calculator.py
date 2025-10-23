import logging
from typing import Dict
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class SupplyCalculator:

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self._last_minted: Dict[str, int] = {}
        self.DEFAULT_TOKEN_DECIMALS = 9
        self.SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
        self.STABLECOINS = {'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 'USDH': 'USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX'}
        self.TOKEN_DECIMALS = {self.SOL_ADDRESS: 9, self.STABLECOINS['USDC']: 6, self.STABLECOINS['USDT']: 6, self.STABLECOINS['USDH']: 6}

    def calculate_supply(self, token_address: str) -> int:
        try:
            total_minted = self._get_total_minted(token_address)
            total_burned = self._get_total_burned(token_address)
            supply = max(0, total_minted - total_burned)
            logger.info(f'Token {token_address[:8]}... - Minted: {total_minted}, Burned: {total_burned}, Supply: {supply}')
            return supply
        except Exception as e:
            logger.error(f'Failed to calculate supply for {token_address}: {e}')
            return 0

    def calculate_supplies_batch(self, token_addresses: list, decimals_map: Dict[str, int] | None=None) -> Dict[str, float]:
        if not token_addresses:
            return {}
        logger.info(f'Calculating supply for {len(token_addresses)} tokens (batch mode)')
        minted_amounts = self._get_minted_batch()
        self._last_minted = minted_amounts
        burned_amounts = self._get_burned_batch()
        supplies: Dict[str, float] = {}
        for token in token_addresses:
            key = token.decode('utf-8', errors='ignore') if isinstance(token, (bytes, bytearray)) else str(token)
            key = key.replace('\x00', '').strip()
            minted_raw = int(minted_amounts.get(key, minted_amounts.get(token, 0)))
            burned_raw = int(burned_amounts.get(key, burned_amounts.get(token, 0)))
            if decimals_map is not None:
                decimals = int(decimals_map.get(key, 6))
            else:
                decimals = self.TOKEN_DECIMALS.get(key, self.DEFAULT_TOKEN_DECIMALS)
            supply_final = (minted_raw - burned_raw) / 10 ** decimals
            supplies[key] = max(0.0, float(supply_final))
        logger.info(f'Calculated supply for {len(supplies)} tokens')
        logger.info(f'Supplies: {supplies}')
        return supplies

    def get_last_initial_minted(self) -> Dict[str, int]:
        return dict(self._last_minted)

    def get_last_initial_minted_normalized(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for k, v in (self._last_minted or {}).items():
            key = k.decode('utf-8', errors='ignore') if isinstance(k, (bytes, bytearray)) else str(k)
            key = key.replace('\x00', '').strip()
            decimals = self.TOKEN_DECIMALS.get(key, self.DEFAULT_TOKEN_DECIMALS)
            out[key] = float(v) / float(10 ** decimals)
        return out

    def _get_total_minted(self, token_address: str) -> int:
        query = '\n        SELECT COALESCE(SUM(amount), 0) as total_minted\n        FROM solana.mints\n        WHERE mint = {mint:String}\n        '
        try:
            result = self.db_client.execute_query(query, parameters={'mint': token_address})
            return int(result[0][0]) if result else 0
        except Exception as e:
            logger.error(f'Failed to get minted amount for {token_address}: {e}')
            return 0

    def _get_total_burned(self, token_address: str) -> int:
        query = '\n        SELECT COALESCE(SUM(amount), 0) as total_burned\n        FROM solana.burns\n        WHERE mint = {mint:String}\n        '
        try:
            result = self.db_client.execute_query(query, parameters={'mint': token_address})
            return int(result[0][0]) if result else 0
        except Exception as e:
            logger.error(f'Failed to get burned amount for {token_address}: {e}')
            return 0

    def _get_minted_batch(self) -> Dict[str, int]:
        query = '\n        SELECT mint, SUM(amount) AS total_minted\n        FROM solana.mints\n        GROUP BY mint\n        '
        logger.info('Executing minted aggregation for all tokens')
        try:
            result = self.db_client.execute_query(query)
            return {row[0]: int(row[1]) for row in result}
        except Exception as e:
            logger.error(f'Failed to get total minted amounts: {e}')
            return {}

    def _get_burned_batch(self) -> Dict[str, int]:
        query = '\n        SELECT mint, SUM(amount) AS total_burned\n        FROM solana.burns\n        GROUP BY mint\n        '
        logger.info('Executing burned aggregation for all tokens')
        try:
            result = self.db_client.execute_query(query)
            return {row[0]: int(row[1]) for row in result}
        except Exception as e:
            logger.error(f'Failed to get total burned amounts: {e}')
            return {}