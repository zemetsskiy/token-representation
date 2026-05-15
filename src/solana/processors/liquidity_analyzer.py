import logging
from typing import List, Dict
import polars as pl
from ...database import ClickHouseClient
from ...config import Config

logger = logging.getLogger(__name__)

STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
LST_ADDRESSES = {
    'JitoSOL': 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',
    'jupSOL': 'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',
    'mSOL': 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
    'bSOL': 'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',
}


class LiquidityAnalyzer:
    """
    Chunk-optimized liquidity analyzer.
    Makes exactly 1 database query per chunk to get pool data.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = None  # Must be set via set_sol_price() before use

    def get_comprehensive_swap_data_for_chunk(self, interval_days: int = 7) -> Dict[str, List[Dict]]:
        """
        Get ALL swap-related data for tokens in chunk_tokens table.
        Uses consolidated query for VWAP prices + first_swaps,
        and separate per-pool query for multi-pool liquidity data.

        Returns:
            Dict with keys:
                - 'pool_data': List of pool metrics (per-pool, not per-token)
                - 'first_swaps': List of first swap dates
                - 'prices': List of VWAP price data with method info
        """
        logger.info('Fetching comprehensive swap data from chunk_tokens table (CONSOLIDATED query + per-pool query)')

        comprehensive_data = self._get_comprehensive_swap_data(interval_days=interval_days)

        logger.info(f'Comprehensive swap query completed: {len(comprehensive_data)} token records (interval: {interval_days}d)')

        first_swaps = []
        prices = []
        price_methods = {}

        for row in comprehensive_data:
            token = row['token']

            if row.get('first_swap'):
                first_swaps.append({
                    'token': token,
                    'first_swap': row['first_swap']
                })

            if row.get('price_raw') and row['price_raw'] > 0:
                price_method = row.get('price_method', 'UNKNOWN')
                price_methods[price_method] = price_methods.get(price_method, 0) + 1

                prices.append({
                    'token': token,
                    'price_reference': row['latest_price_reference'],
                    'price_reference_type': row.get('price_reference_type', 'STABLE'),
                    'price_raw': row['price_raw'],
                    'price_method': price_method,
                    'base_coin': row['latest_base_coin'],
                    'quote_coin': row['latest_quote_coin'],
                    'base_balance': row['latest_base_balance'],
                    'quote_balance': row['latest_quote_balance'],
                    'trades_5m': row.get('trades_5m', 0),
                    'trades_1h': row.get('trades_1h', 0),
                    'trades_24h': row.get('trades_24h', 0),
                })

        if price_methods:
            method_str = ', '.join([f'{k}: {v}' for k, v in sorted(price_methods.items())])
            logger.info(f'Price methods used: {method_str}')

        pool_data = self._get_pools_for_chunk(interval_days=interval_days)

        logger.info(f'Extracted: {len(first_swaps)} first swaps, {len(pool_data)} pools, {len(prices)} VWAP prices')

        return {
            'pool_data': pool_data,
            'first_swaps': first_swaps,
            'prices': prices
        }

    def _get_comprehensive_swap_data(self, interval_days: int = 7) -> List[Dict]:
        """
        CONSOLIDATED QUERY: Get ALL swap data (first_swap, pools, VWAP prices) in ONE query.
        Uses Trade-Based VWAP pricing with cascading fallback for accuracy.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']

        allowed_sources = [
            'pumpfun_bondingcurve',
            'raydium_swap_v4',
            'raydium_swap_cpmm',
            'raydium_swap_clmm',
            'raydium_swap_stable',
            'raydium_bondingcurve',
            'meteora_swap_dlmm',
            'meteora_swap_pools',
            'meteora_swap_damm',
            'meteora_bondingcurve',
            'orca_swap',
            'phoenix_swap',
            'lifinity_swap_v2',
            'pumpswap_swap',
            'degenfund',
        ]
        allowed_sources_sql = ', '.join([f"'{s}'" for s in allowed_sources])

        query = f"""
        WITH
        -- 1. Unify swaps in SINGLE scan using conditional logic
        -- OPTIMIZATION: block_time filter in PREWHERE reduces data scan significantly
        unified_swaps AS (
            SELECT
                -- Token is whichever side is in our chunk (not SOL/STABLE)
                if(base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens), base_coin, quote_coin) AS token,
                source,
                base_coin,
                quote_coin,
                block_time,
                -- Token amount depends on which side the token is
                if(base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens), base_coin_amount, quote_coin_amount) AS token_amount,
                -- Reference amount is the other side
                if(base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens), quote_coin_amount, base_coin_amount) AS ref_amount,
                -- Reference type
                multiIf(
                    base_coin = '{SOL_ADDRESS}' OR quote_coin = '{SOL_ADDRESS}', 'SOL',
                    base_coin IN ('{usdc}', '{usdt}') OR quote_coin IN ('{usdc}', '{usdt}'), 'STABLE',
                    'OTHER'
                ) AS ref_type,
                -- Pool balances
                base_pool_balance_after,
                quote_pool_balance_after,
                -- Reference balance for liquidity
                if(base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens), quote_pool_balance_after, base_pool_balance_after) AS ref_balance_raw
            FROM solana.swaps
            PREWHERE
                block_time >= now() - INTERVAL {interval_days} DAY
                AND (
                    (base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens) AND (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}')))
                    OR
                    (quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens) AND (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}')))
                )
            WHERE source IN ({allowed_sources_sql})
              AND base_coin_amount > 0
              AND quote_coin_amount > 0
        ),

        -- 2. Calculate VWAP from ALL trades, separately for SOL and STABLE
        --    Then pick the ref_type with more recent activity
        token_vwap AS (
            SELECT
                token,

                -- STABLE VWAP
                sumIf(ref_amount, block_time >= now() - INTERVAL 5 MINUTE AND ref_type = 'STABLE')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 5 MINUTE AND ref_type = 'STABLE'), 1) AS stable_vwap_5m,
                sumIf(ref_amount, block_time >= now() - INTERVAL 1 HOUR AND ref_type = 'STABLE')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 1 HOUR AND ref_type = 'STABLE'), 1) AS stable_vwap_1h,
                sumIf(ref_amount, block_time >= now() - INTERVAL 24 HOUR AND ref_type = 'STABLE')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 24 HOUR AND ref_type = 'STABLE'), 1) AS stable_vwap_24h,
                sumIf(ref_amount, block_time >= now() - INTERVAL 7 DAY AND ref_type = 'STABLE')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 7 DAY AND ref_type = 'STABLE'), 1) AS stable_vwap_7d,
                argMaxIf(ref_amount / token_amount, block_time, ref_type = 'STABLE') AS stable_last,
                countIf(block_time >= now() - INTERVAL 5 MINUTE AND ref_type = 'STABLE') AS stable_trades_5m,
                countIf(block_time >= now() - INTERVAL 1 HOUR AND ref_type = 'STABLE') AS stable_trades_1h,
                countIf(block_time >= now() - INTERVAL 24 HOUR AND ref_type = 'STABLE') AS stable_trades_24h,
                countIf(block_time >= now() - INTERVAL 7 DAY AND ref_type = 'STABLE') AS stable_trades_7d,

                -- SOL VWAP
                sumIf(ref_amount, block_time >= now() - INTERVAL 5 MINUTE AND ref_type = 'SOL')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 5 MINUTE AND ref_type = 'SOL'), 1) AS sol_vwap_5m,
                sumIf(ref_amount, block_time >= now() - INTERVAL 1 HOUR AND ref_type = 'SOL')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 1 HOUR AND ref_type = 'SOL'), 1) AS sol_vwap_1h,
                sumIf(ref_amount, block_time >= now() - INTERVAL 24 HOUR AND ref_type = 'SOL')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 24 HOUR AND ref_type = 'SOL'), 1) AS sol_vwap_24h,
                sumIf(ref_amount, block_time >= now() - INTERVAL 7 DAY AND ref_type = 'SOL')
                    / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 7 DAY AND ref_type = 'SOL'), 1) AS sol_vwap_7d,
                argMaxIf(ref_amount / token_amount, block_time, ref_type = 'SOL') AS sol_last,
                countIf(block_time >= now() - INTERVAL 5 MINUTE AND ref_type = 'SOL') AS sol_trades_5m,
                countIf(block_time >= now() - INTERVAL 1 HOUR AND ref_type = 'SOL') AS sol_trades_1h,
                countIf(block_time >= now() - INTERVAL 24 HOUR AND ref_type = 'SOL') AS sol_trades_24h,
                countIf(block_time >= now() - INTERVAL 7 DAY AND ref_type = 'SOL') AS sol_trades_7d,

                -- Latest pool state from most recent trade (not peak liquidity)
                argMax(source, block_time) AS best_source,
                argMax(base_coin, block_time) AS best_base_coin,
                argMax(quote_coin, block_time) AS best_quote_coin,
                argMax(base_pool_balance_after, block_time) AS best_base_balance,
                argMax(quote_pool_balance_after, block_time) AS best_quote_balance,
                argMax(CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END, block_time) AS liquidity_usd,

                min(block_time) AS first_swap_time
            FROM unified_swaps
            WHERE ref_type != 'OTHER'
            GROUP BY token
        )

        -- 3. Final selection - pick ref_type with more trades, cascading VWAP
        SELECT
            token,
            first_swap_time AS first_swap,
            best_source AS latest_source,
            best_base_coin AS latest_base_coin,
            best_quote_coin AS latest_quote_coin,
            best_base_balance AS latest_base_balance,
            best_quote_balance AS latest_quote_balance,

            -- Pick VWAP from ref_type with more 24h trades (more active = more reliable price)
            multiIf(
                -- SOL has more trades - use SOL VWAP (cascade: 5m → 1h → 24h → 7d → last)
                sol_trades_7d > stable_trades_7d AND sol_trades_5m >= 3, sol_vwap_5m,
                sol_trades_7d > stable_trades_7d AND sol_trades_1h >= 5, sol_vwap_1h,
                sol_trades_7d > stable_trades_7d AND sol_trades_24h >= 5, sol_vwap_24h,
                sol_trades_7d > stable_trades_7d AND sol_trades_7d >= 10, sol_vwap_7d,
                sol_trades_7d > stable_trades_7d AND sol_last > 0, sol_last,
                -- STABLE has more trades
                stable_trades_7d >= sol_trades_7d AND stable_trades_5m >= 3, stable_vwap_5m,
                stable_trades_7d >= sol_trades_7d AND stable_trades_1h >= 5, stable_vwap_1h,
                stable_trades_7d >= sol_trades_7d AND stable_trades_24h >= 5, stable_vwap_24h,
                stable_trades_7d >= sol_trades_7d AND stable_trades_7d >= 10, stable_vwap_7d,
                stable_trades_7d >= sol_trades_7d AND stable_last > 0, stable_last,
                -- Fallback to any available
                sol_last > 0, sol_last,
                stable_last > 0, stable_last,
                0
            ) AS price_raw,

            -- Price method
            multiIf(
                sol_trades_7d > stable_trades_7d AND sol_trades_5m >= 3, 'SOL_VWAP_5M',
                sol_trades_7d > stable_trades_7d AND sol_trades_1h >= 5, 'SOL_VWAP_1H',
                sol_trades_7d > stable_trades_7d AND sol_trades_24h >= 5, 'SOL_VWAP_24H',
                sol_trades_7d > stable_trades_7d AND sol_trades_7d >= 10, 'SOL_VWAP_7D',
                sol_trades_7d > stable_trades_7d AND sol_last > 0, 'SOL_LAST',
                stable_trades_7d >= sol_trades_7d AND stable_trades_5m >= 3, 'STABLE_VWAP_5M',
                stable_trades_7d >= sol_trades_7d AND stable_trades_1h >= 5, 'STABLE_VWAP_1H',
                stable_trades_7d >= sol_trades_7d AND stable_trades_24h >= 5, 'STABLE_VWAP_24H',
                stable_trades_7d >= sol_trades_7d AND stable_trades_7d >= 10, 'STABLE_VWAP_7D',
                stable_trades_7d >= sol_trades_7d AND stable_last > 0, 'STABLE_LAST',
                sol_last > 0, 'SOL_LAST',
                stable_last > 0, 'STABLE_LAST',
                'NONE'
            ) AS price_method,

            -- Reference type
            multiIf(
                sol_trades_7d > stable_trades_7d AND (sol_trades_5m >= 3 OR sol_trades_1h >= 5 OR sol_trades_24h >= 5 OR sol_trades_7d >= 10 OR sol_last > 0), 'SOL',
                stable_trades_7d >= sol_trades_7d AND (stable_trades_5m >= 3 OR stable_trades_1h >= 5 OR stable_trades_24h >= 5 OR stable_trades_7d >= 10 OR stable_last > 0), 'STABLE',
                sol_last > 0, 'SOL',
                stable_last > 0, 'STABLE',
                'NONE'
            ) AS price_reference_type,

            -- Reference coin address
            multiIf(
                sol_trades_7d > stable_trades_7d AND (sol_trades_5m >= 3 OR sol_trades_1h >= 5 OR sol_trades_24h >= 5 OR sol_trades_7d >= 10 OR sol_last > 0), '{SOL_ADDRESS}',
                stable_trades_7d >= sol_trades_7d AND (stable_trades_5m >= 3 OR stable_trades_1h >= 5 OR stable_trades_24h >= 5 OR stable_trades_7d >= 10 OR stable_last > 0), '{usdc}',
                sol_last > 0, '{SOL_ADDRESS}',
                stable_last > 0, '{usdc}',
                ''
            ) AS latest_price_reference,

            -- Liquidity
            liquidity_usd,

            -- Trade counts (from selected ref_type)
            if(sol_trades_7d > stable_trades_7d, sol_trades_5m, stable_trades_5m) AS trades_5m,
            if(sol_trades_7d > stable_trades_7d, sol_trades_1h, stable_trades_1h) AS trades_1h,
            if(sol_trades_7d > stable_trades_7d, sol_trades_24h, stable_trades_24h) AS trades_24h

        FROM token_vwap
        WHERE price_raw > 0
        """

        logger.debug(f'Executing CONSOLIDATED swap aggregation from {temp_db}.chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)

            decoded_result = []
            for row in result:
                token_value = row['token']
                base_coin_value = row['latest_base_coin']
                quote_coin_value = row['latest_quote_coin']

                token_str = token_value.decode('utf-8').rstrip('\x00') if isinstance(token_value, bytes) else str(token_value).rstrip('\x00')
                base_coin_str = base_coin_value.decode('utf-8').rstrip('\x00') if isinstance(base_coin_value, bytes) else str(base_coin_value).rstrip('\x00')
                quote_coin_str = quote_coin_value.decode('utf-8').rstrip('\x00') if isinstance(quote_coin_value, bytes) else str(quote_coin_value).rstrip('\x00')

                price_reference_value = row['latest_price_reference']
                if isinstance(price_reference_value, bytes):
                    price_reference_str = price_reference_value.decode('utf-8').rstrip('\x00')
                else:
                    price_reference_str = (
                        str(price_reference_value).rstrip('\x00') if price_reference_value is not None else None
                    )

                decoded_row = {
                    'token': token_str,
                    'first_swap': row['first_swap'],
                    'latest_source': row['latest_source'],
                    'latest_base_coin': base_coin_str,
                    'latest_quote_coin': quote_coin_str,
                    'latest_base_balance': row['latest_base_balance'],
                    'latest_quote_balance': row['latest_quote_balance'],
                    'price_raw': row['price_raw'],  # VWAP price (raw, needs decimal conversion)
                    'price_method': row['price_method'],  # Which method was used
                    'price_reference_type': row['price_reference_type'],  # SOL or STABLE
                    'latest_price_reference': price_reference_str,
                    'liquidity_usd': row['liquidity_usd'],
                    'trades_5m': row['trades_5m'],
                    'trades_1h': row['trades_1h'],
                    'trades_24h': row['trades_24h'],
                }
                decoded_result.append(decoded_row)

            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get comprehensive swap data: {e}', exc_info=True)
            return []

    def _get_pools_for_chunk(self, interval_days: int = 7) -> List[Dict]:
        """Query per-pool latest balances for tokens in chunk_tokens table."""
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        lst_addresses = list(LST_ADDRESSES.values())
        lst_sql = ', '.join([f"'{a}'" for a in lst_addresses])

        allowed_sources = [
            'pumpfun_bondingcurve',
            'raydium_swap_v4', 'raydium_swap_cpmm', 'raydium_swap_clmm',
            'raydium_swap_stable', 'raydium_bondingcurve',
            'meteora_swap_dlmm', 'meteora_swap_damm', 'meteora_bondingcurve',
            'orca_swap', 'phoenix_swap', 'lifinity_swap_v2',
            'pumpswap_swap', 'degenfund',
        ]
        allowed_sources_sql = ', '.join([f"'{s}'" for s in allowed_sources])

        query = f"""
        SELECT
            source AS canonical_source,
            base_coin,
            quote_coin,
            argMax(base_pool_balance_after, block_time) AS last_base_balance,
            argMax(quote_pool_balance_after, block_time) AS last_quote_balance,
            max(block_time) AS last_trade_time
        FROM solana.swaps
        PREWHERE block_time >= now() - INTERVAL {interval_days} DAY
        WHERE
            source IN ({allowed_sources_sql})
            AND (
                (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}') OR quote_coin IN ({lst_sql}))
                OR
                (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}') OR base_coin IN ({lst_sql}))
            )
            AND (base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens) OR quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens))
            AND base_coin_amount > 0
            AND quote_coin_amount > 0
        GROUP BY canonical_source, base_coin, quote_coin
        HAVING last_base_balance > 0
            AND last_quote_balance > 0
        """

        logger.debug(f'Executing pool aggregation from {temp_db}.chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)
            decoded_result = []
            for row in result:
                base_coin_value = row['base_coin']
                quote_coin_value = row['quote_coin']

                if isinstance(base_coin_value, bytes):
                    base_coin_str = base_coin_value.decode('utf-8').rstrip('\x00')
                else:
                    base_coin_str = str(base_coin_value).rstrip('\x00')

                if isinstance(quote_coin_value, bytes):
                    quote_coin_str = quote_coin_value.decode('utf-8').rstrip('\x00')
                else:
                    quote_coin_str = str(quote_coin_value).rstrip('\x00')

                decoded_result.append({
                    'canonical_source': row['canonical_source'],
                    'base_coin': base_coin_str,
                    'quote_coin': quote_coin_str,
                    'last_base_balance': row['last_base_balance'],
                    'last_quote_balance': row['last_quote_balance'],
                    'last_trade_time': row['last_trade_time'],
                })
            logger.info(f'Pool query returned {len(decoded_result)} pools (with LST pairs + latest balance)')
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get pool metrics: {e}', exc_info=True)
            return []

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
