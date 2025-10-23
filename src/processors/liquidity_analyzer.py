import logging
import json
from typing import Dict, Optional
from collections import defaultdict
from ..database import ClickHouseClient

logger = logging.getLogger(__name__)

STABLECOINS = {'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'}
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_PRICE_USD = 190.0

class LiquidityAnalyzer:

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_best_pool_metrics_batch(self, token_addresses: list, decimals_map: Dict[str, int]) -> Dict[str, dict]:
        if not token_addresses:
            return {}
        normalized_tokens = []
        for token in token_addresses:
            t = token.decode('utf-8', errors='ignore') if isinstance(token, (bytes, bytearray)) else str(token)
            t = t.replace('\x00', '').strip()
            if t:
                normalized_tokens.append(t)
        candidate_pools_raw = self._get_all_candidate_pools_batch(normalized_tokens) or []
        pools_by_token: Dict[str, Dict[str, list]] = defaultdict(lambda: {'priority': [], 'bonding': []})
        for row in candidate_pools_raw:
            source, base_coin, quote_coin, base_balance_raw, quote_balance_raw = row
            source, base_coin, quote_coin = map(lambda x: x.decode('utf-8', 'ignore').strip('\x00') if isinstance(x, bytes) else str(x), [source, base_coin, quote_coin])
            base_decimals = decimals_map.get(base_coin, 9 if base_coin == SOL_ADDRESS else 6)
            quote_decimals = decimals_map.get(quote_coin, 9 if quote_coin == SOL_ADDRESS else 6)
            base_balance_norm = float(base_balance_raw) / 10 ** base_decimals
            quote_balance_norm = float(quote_balance_raw) / 10 ** quote_decimals
            liquidity_usd = 0.0
            if base_coin == SOL_ADDRESS:
                liquidity_usd = base_balance_norm * float(self.sol_price_usd) * 2.0
            elif quote_coin == SOL_ADDRESS:
                liquidity_usd = quote_balance_norm * float(self.sol_price_usd) * 2.0
            elif base_coin in STABLECOINS.values():
                liquidity_usd = base_balance_norm * 2.0
            elif quote_coin in STABLECOINS.values():
                liquidity_usd = quote_balance_norm * 2.0
            pool_data = {'source': source, 'base_coin': base_coin, 'quote_coin': quote_coin, 'base_balance_norm': base_balance_norm, 'quote_balance_norm': quote_balance_norm, 'liquidity_usd': liquidity_usd}
            pool_category = 'bonding' if 'bondingcurve' in source.lower() else 'priority'
            if base_coin in normalized_tokens:
                pools_by_token[base_coin][pool_category].append(pool_data)
            if quote_coin in normalized_tokens:
                pools_by_token[quote_coin][pool_category].append(pool_data)
        final_metrics: Dict[str, dict] = {}
        for token in normalized_tokens:
            token_pools = pools_by_token.get(token)
            best_pool = None
            if token_pools:
                candidate_list = token_pools['priority'] if token_pools['priority'] else token_pools['bonding']
                if candidate_list:
                    best_pool = max(candidate_list, key=lambda p: p['liquidity_usd'])
            if not best_pool:
                final_metrics[token] = {'source': '', 'liquidity_usd': 0.0, 'price_usd': 0.0}
                continue
            price_usd = 0.0
            if best_pool['base_balance_norm'] > 0 and best_pool['quote_balance_norm'] > 0:
                if token == best_pool['base_coin']:
                    quote_val_usd = best_pool['quote_balance_norm'] * (float(self.sol_price_usd) if best_pool['quote_coin'] == SOL_ADDRESS else 1.0)
                    price_usd = quote_val_usd / best_pool['base_balance_norm']
                else:
                    base_val_usd = best_pool['base_balance_norm'] * (float(self.sol_price_usd) if best_pool['base_coin'] == SOL_ADDRESS else 1.0)
                    price_usd = base_val_usd / best_pool['quote_balance_norm']
            final_metrics[token] = {'source': best_pool['source'], 'liquidity_usd': best_pool['liquidity_usd'], 'price_usd': price_usd}
        return final_metrics

    def _get_all_candidate_pools_batch(self, token_addresses: list) -> list:
        if not token_addresses:
            return []
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']
        query = f"\n        SELECT\n            CASE\n                WHEN source LIKE 'jupiter6_%' THEN substring(source, 10)\n                WHEN source LIKE 'jupiter4_%' THEN substring(source, 10)\n                WHEN source LIKE 'raydium_route_%' THEN substring(source, 15)\n                ELSE source\n            END AS canonical_source,\n            base_coin,\n            quote_coin,\n            argMax(base_pool_balance_after, block_time) AS last_base_balance,\n            argMax(quote_pool_balance_after, block_time) AS last_quote_balance\n        FROM solana.swaps\n        WHERE\n            (base_coin IN ({placeholders}) AND (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}','{usdt}')))\n            OR\n            (quote_coin IN ({placeholders}) AND (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}','{usdt}')))\n        GROUP BY canonical_source, base_coin, quote_coin\n        HAVING last_base_balance > 0 AND last_quote_balance > 0\n        "
        try:
            result = self.db_client.execute_query(query) or []
            logger.info(f'Received {len(result)} candidate pools from DB.')
            return result
        except Exception as e:
            logger.error(f'Failed to get candidate pools batch: {e}')
            return []

    def get_token_reserves_map(self, token_addresses: list, decimals_map: Dict[str, int]) -> Dict[str, float]:
        if not token_addresses:
            return {}
        normalized_tokens = []
        for token in token_addresses:
            t = token.decode('utf-8', errors='ignore') if isinstance(token, (bytes, bytearray)) else str(token)
            t = t.replace('\x00', '').strip()
            if t:
                normalized_tokens.append(t)
        candidate_pools_raw = self._get_all_candidate_pools_batch(normalized_tokens) or []
        reserves: Dict[str, float] = {t: 0.0 for t in normalized_tokens}
        for row in candidate_pools_raw:
            source, base_coin, quote_coin, base_balance_raw, quote_balance_raw = row
            if isinstance(base_coin, (bytes, bytearray)):
                base_coin = base_coin.decode('utf-8', errors='ignore').replace('\x00', '').strip()
            if isinstance(quote_coin, (bytes, bytearray)):
                quote_coin = quote_coin.decode('utf-8', errors='ignore').replace('\x00', '').strip()
            base_decimals = int(decimals_map.get(base_coin, 9 if base_coin == SOL_ADDRESS else 6))
            quote_decimals = int(decimals_map.get(quote_coin, 9 if quote_coin == SOL_ADDRESS else 6))
            try:
                base_balance_norm = float(base_balance_raw) / 10 ** base_decimals
            except Exception:
                base_balance_norm = float(int(base_balance_raw)) / 10 ** base_decimals
            try:
                quote_balance_norm = float(quote_balance_raw) / 10 ** quote_decimals
            except Exception:
                quote_balance_norm = float(int(quote_balance_raw)) / 10 ** quote_decimals
            if base_coin in reserves:
                reserves[base_coin] += base_balance_norm
            if quote_coin in reserves:
                reserves[quote_coin] += quote_balance_norm
        return reserves

    def set_sol_price(self, price: float):
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')