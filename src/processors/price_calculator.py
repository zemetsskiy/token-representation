import logging
from typing import Dict, Optional
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

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def calculate_price(self, token_address: str) -> float:
        try:
            if self.sol_price_usd is None:
                self.sol_price_usd = self._get_sol_price()
            pool_info = self._find_most_liquid_pool(token_address)
            if not pool_info:
                logger.warning(f'No liquid pool found for token {token_address[:8]}...')
                return 0.0
            price = self._calculate_price_from_pool(token_address, pool_info)
            logger.debug(f'Token {token_address[:8]}... price: ${price:.6f}')
            return price
        except Exception as e:
            logger.error(f'Failed to calculate price for {token_address}: {e}')
            return 0.0

    def calculate_prices_batch(self, token_addresses: list) -> Dict[str, float]:
        if not token_addresses:
            return {}
        logger.info(f'Calculating prices for {len(token_addresses)} tokens (batch)')
        sol_price = self.get_sol_price()
        last_prices_in_sol = self._get_latest_prices_batch(token_addresses)
        prices: Dict[str, float] = {}
        for token, price_in_sol in last_prices_in_sol.items():
            if price_in_sol is None:
                prices[token] = 0.0
            else:
                prices[token] = float(price_in_sol) * float(sol_price)
        logger.info(f'Calculated prices for {len(prices)} tokens (batch)')
        return prices

    def _get_latest_prices_batch(self, token_addresses: list) -> Dict[str, Optional[float]]:
        if not token_addresses:
            return {}
        normalized_tokens = []
        for token in token_addresses:
            t = token.decode('utf-8', errors='ignore') if isinstance(token, (bytes, bytearray)) else str(token)
            t = t.replace('\x00', '').strip()
            if t:
                normalized_tokens.append(t)
        placeholders = ', '.join([f"'{t}'" for t in normalized_tokens])
        query = f"\n        SELECT\n            token,\n            argMax(price, block_time) AS last_price_in_sol\n        FROM (\n            -- token is base vs SOL\n            SELECT\n                base_coin AS token,\n                block_time,\n                quote_coin_amount / NULLIF(base_coin_amount, 0) AS price\n            FROM solana.swaps\n            WHERE quote_coin = '{SOL_ADDRESS}' AND base_coin IN ({placeholders})\n\n            UNION ALL\n\n            -- token is quote vs SOL\n            SELECT\n                quote_coin AS token,\n                block_time,\n                base_coin_amount / NULLIF(quote_coin_amount, 0) AS price\n            FROM solana.swaps\n            WHERE base_coin = '{SOL_ADDRESS}' AND quote_coin IN ({placeholders})\n        )\n        GROUP BY token\n        "
        try:
            result = self.db_client.execute_query(query)
            return {row[0]: float(row[1]) if row[1] is not None else None for row in result}
        except Exception as e:
            logger.error(f'Failed to get latest prices batch: {e}')
            return {t: None for t in normalized_tokens}

    def _get_sol_price(self) -> float:
        return SOL_PRICE_USD

    def get_sol_price(self) -> float:
        if self.sol_price_usd is None:
            self.sol_price_usd = self._get_sol_price()
        return self.sol_price_usd

    def _find_most_liquid_pool(self, token_address: str) -> Optional[Dict]:
        query = '\n        SELECT\n            base_coin,\n            quote_coin,\n            base_pool_balance_after,\n            quote_pool_balance_after,\n            base_coin_amount,\n            quote_coin_amount\n        FROM solana.swaps\n        WHERE (base_coin = {token:String} OR quote_coin = {token:String})\n        ORDER BY block_time DESC\n        LIMIT 100\n        '
        try:
            result = self.db_client.execute_query(query, parameters={'token': token_address})
            if not result:
                return None
            best_pool = None
            max_liquidity = 0
            for row in result:
                base_coin, quote_coin, base_balance, quote_balance, base_amount, quote_amount = row
                liquidity_usd = self._estimate_pool_liquidity(base_coin, quote_coin, base_balance, quote_balance)
                if liquidity_usd > max_liquidity:
                    max_liquidity = liquidity_usd
                    best_pool = {'base_coin': base_coin, 'quote_coin': quote_coin, 'base_balance': float(base_balance), 'quote_balance': float(quote_balance), 'base_amount': float(base_amount), 'quote_amount': float(quote_amount), 'liquidity_usd': liquidity_usd}
            return best_pool
        except Exception as e:
            logger.error(f'Failed to find liquid pool for {token_address}: {e}')
            return None

    def _estimate_pool_liquidity(self, base_coin: str, quote_coin: str, base_balance: float, quote_balance: float) -> float:
        liquidity = 0.0
        if base_coin == SOL_ADDRESS:
            liquidity += base_balance * self.sol_price_usd
        elif quote_coin == SOL_ADDRESS:
            liquidity += quote_balance * self.sol_price_usd
        if base_coin in STABLECOINS.values():
            liquidity += base_balance
        elif quote_coin in STABLECOINS.values():
            liquidity += quote_balance
        return liquidity

    def _calculate_price_from_pool(self, token_address: str, pool_info: Dict) -> float:
        base_coin = pool_info['base_coin']
        quote_coin = pool_info['quote_coin']
        base_amount = pool_info['base_amount']
        quote_amount = pool_info['quote_amount']
        if base_amount == 0 or quote_amount == 0:
            return 0.0
        if base_coin == token_address:
            price_in_quote = quote_amount / base_amount
            if quote_coin == SOL_ADDRESS:
                return price_in_quote * self.sol_price_usd
            elif quote_coin in STABLECOINS.values():
                return price_in_quote
            else:
                return 0.0
        elif quote_coin == token_address:
            price_in_base = base_amount / quote_amount
            if base_coin == SOL_ADDRESS:
                return price_in_base * self.sol_price_usd
            elif base_coin in STABLECOINS.values():
                return price_in_base
            else:
                return 0.0
        return 0.0