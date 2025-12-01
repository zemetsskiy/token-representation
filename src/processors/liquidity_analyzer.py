import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
from ..config import Config

logger = logging.getLogger(__name__)

STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
# SOL price is fetched from Redis at runtime - no hardcoded fallback


class LiquidityAnalyzer:
    """
    Chunk-optimized liquidity analyzer.
    Makes exactly 1 database query per chunk to get pool data.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = None  # Must be set via set_sol_price() before use

    def get_comprehensive_swap_data_for_chunk(self) -> Dict[str, List[Dict]]:
        """
        Get ALL swap-related data for tokens in chunk_tokens table using ONE powerful query.
        This consolidates: first_swap dates, pool metrics, and VWAP price data.

        Returns:
            Dict with keys:
                - 'pool_data': List of pool metrics
                - 'first_swaps': List of first swap dates
                - 'prices': List of VWAP price data with method info
        """
        logger.info('Fetching comprehensive swap data from chunk_tokens table (1 CONSOLIDATED query with VWAP)')

        comprehensive_data = self._get_comprehensive_swap_data()

        logger.info(f'Comprehensive swap query completed: {len(comprehensive_data)} token records')

        # Separate data into different categories for downstream processing
        pool_data = []
        first_swaps = []
        prices = []

        # Track price method distribution for logging
        price_methods = {}

        for row in comprehensive_data:
            token = row['token']

            # First swap data
            if row.get('first_swap'):
                first_swaps.append({
                    'token': token,
                    'first_swap': row['first_swap']
                })

            # Pool data (if this token has pool info)
            if row.get('latest_source') and row.get('latest_base_balance', 0) > 0:
                pool_data.append({
                    'canonical_source': row['latest_source'],
                    'base_coin': row['latest_base_coin'],
                    'quote_coin': row['latest_quote_coin'],
                    'last_base_balance': row['latest_base_balance'],
                    'last_quote_balance': row['latest_quote_balance'],
                    'liquidity_usd': row.get('liquidity_usd', 0)
                })

            # Price data - now using VWAP from trade amounts
            if row.get('price_raw') and row['price_raw'] > 0:
                price_method = row.get('price_method', 'UNKNOWN')
                price_methods[price_method] = price_methods.get(price_method, 0) + 1

                prices.append({
                    'token': token,
                    'price_reference': row['latest_price_reference'],
                    'price_reference_type': row.get('price_reference_type', 'STABLE'),
                    'price_raw': row['price_raw'],  # VWAP price in reference units
                    'price_method': price_method,
                    # Keep pool info for liquidity calculation
                    'base_coin': row['latest_base_coin'],
                    'quote_coin': row['latest_quote_coin'],
                    'base_balance': row['latest_base_balance'],
                    'quote_balance': row['latest_quote_balance'],
                    # Trade activity info
                    'trades_5m': row.get('trades_5m', 0),
                    'trades_1h': row.get('trades_1h', 0),
                    'trades_24h': row.get('trades_24h', 0),
                })

        # Log price method distribution
        if price_methods:
            method_str = ', '.join([f'{k}: {v}' for k, v in sorted(price_methods.items())])
            logger.info(f'Price methods used: {method_str}')

        logger.info(f'Extracted: {len(first_swaps)} first swaps, {len(pool_data)} pools, {len(prices)} VWAP prices')

        return {
            'pool_data': pool_data,
            'first_swaps': first_swaps,
            'prices': prices
        }

    def _get_comprehensive_swap_data(self) -> List[Dict]:
        """
        CONSOLIDATED QUERY: Get ALL swap data (first_swap, pools, VWAP prices) in ONE query.
        Uses Trade-Based VWAP pricing with cascading fallback for accuracy.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']

        # ONLY include direct DEX sources - exclude aggregators
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
        -- 1. Unify swaps: normalize token/reference amounts for VWAP calculation
        unified_swaps AS (
            SELECT
                base_coin AS token,
                source,
                base_coin,
                quote_coin,
                block_time,
                -- Token amount (the token we're pricing)
                base_coin_amount AS token_amount,
                -- Reference amount (SOL or stablecoin)
                quote_coin_amount AS ref_amount,
                -- Reference type for USD conversion
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN 'STABLE'
                    ELSE 'OTHER'
                END as ref_type,
                -- Pool balances for liquidity calculation
                base_pool_balance_after,
                quote_pool_balance_after,
                quote_pool_balance_after AS ref_balance_raw
            FROM solana.swaps
            PREWHERE
                base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
                AND (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}'))
            WHERE source IN ({allowed_sources_sql})
              AND base_coin_amount > 0
              AND quote_coin_amount > 0

            UNION ALL

            SELECT
                quote_coin AS token,
                source,
                base_coin,
                quote_coin,
                block_time,
                -- Token amount (the token we're pricing)
                quote_coin_amount AS token_amount,
                -- Reference amount (SOL or stablecoin)
                base_coin_amount AS ref_amount,
                -- Reference type
                CASE
                    WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN 'STABLE'
                    ELSE 'OTHER'
                END as ref_type,
                -- Pool balances
                base_pool_balance_after,
                quote_pool_balance_after,
                base_pool_balance_after AS ref_balance_raw
            FROM solana.swaps
            PREWHERE
                quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
                AND (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}'))
            WHERE source IN ({allowed_sources_sql})
              AND base_coin_amount > 0
              AND quote_coin_amount > 0
        ),

        -- 2. Find best pool per token (by max liquidity in USD)
        best_pools AS (
            SELECT
                token,
                argMax(source, CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS best_source,
                argMax(base_coin, CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS best_base_coin,
                argMax(quote_coin, CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS best_quote_coin,
                argMax(base_pool_balance_after, CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS best_base_balance,
                argMax(quote_pool_balance_after, CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS best_quote_balance,
                argMax(ref_type, CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS best_ref_type,
                max(CASE
                    WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {self.sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
                    ELSE 0
                END) AS liquidity_usd,
                min(block_time) AS first_swap_time
            FROM unified_swaps
            WHERE ref_type != 'OTHER'
            GROUP BY token
        ),

        -- 3. Calculate VWAP only from trades in the best pool
        pool_vwap AS (
            SELECT
                s.token,
                -- VWAP from best pool trades only
                sumIf(s.ref_amount, s.block_time >= now() - INTERVAL 5 MINUTE)
                    / greatest(sumIf(s.token_amount, s.block_time >= now() - INTERVAL 5 MINUTE), 1) AS vwap_5m_raw,
                sumIf(s.ref_amount, s.block_time >= now() - INTERVAL 1 HOUR)
                    / greatest(sumIf(s.token_amount, s.block_time >= now() - INTERVAL 1 HOUR), 1) AS vwap_1h_raw,
                sumIf(s.ref_amount, s.block_time >= now() - INTERVAL 24 HOUR)
                    / greatest(sumIf(s.token_amount, s.block_time >= now() - INTERVAL 24 HOUR), 1) AS vwap_24h_raw,
                argMax(s.ref_amount / s.token_amount, s.block_time) AS last_price_raw,
                countIf(s.block_time >= now() - INTERVAL 5 MINUTE) AS trades_5m,
                countIf(s.block_time >= now() - INTERVAL 1 HOUR) AS trades_1h,
                countIf(s.block_time >= now() - INTERVAL 24 HOUR) AS trades_24h
            FROM unified_swaps s
            INNER JOIN best_pools bp ON s.token = bp.token
                AND s.source = bp.best_source
                AND s.base_coin = bp.best_base_coin
                AND s.quote_coin = bp.best_quote_coin
            GROUP BY s.token
        )

        -- 4. Final selection with cascading VWAP from best pool
        SELECT
            bp.token,
            bp.first_swap_time AS first_swap,
            bp.best_source AS latest_source,
            bp.best_base_coin AS latest_base_coin,
            bp.best_quote_coin AS latest_quote_coin,
            bp.best_base_balance AS latest_base_balance,
            bp.best_quote_balance AS latest_quote_balance,

            -- Cascading VWAP from best pool
            multiIf(
                pv.trades_5m >= 3, pv.vwap_5m_raw,
                pv.trades_1h >= 5, pv.vwap_1h_raw,
                pv.trades_24h >= 5, pv.vwap_24h_raw,
                pv.last_price_raw > 0, pv.last_price_raw,
                0
            ) AS price_raw,

            -- Price method
            multiIf(
                pv.trades_5m >= 3, concat(bp.best_ref_type, '_VWAP_5M'),
                pv.trades_1h >= 5, concat(bp.best_ref_type, '_VWAP_1H'),
                pv.trades_24h >= 5, concat(bp.best_ref_type, '_VWAP_24H'),
                pv.last_price_raw > 0, concat(bp.best_ref_type, '_LAST'),
                'NONE'
            ) AS price_method,

            -- Reference type for USD conversion
            bp.best_ref_type AS price_reference_type,

            -- Reference coin address
            if(bp.best_ref_type = 'SOL', '{SOL_ADDRESS}', '{usdc}') AS latest_price_reference,

            -- Liquidity
            bp.liquidity_usd,

            -- Trade counts from best pool
            pv.trades_5m,
            pv.trades_1h,
            pv.trades_24h

        FROM best_pools bp
        LEFT JOIN pool_vwap pv ON bp.token = pv.token
        WHERE multiIf(
            pv.trades_5m >= 3, pv.vwap_5m_raw,
            pv.trades_1h >= 5, pv.vwap_1h_raw,
            pv.trades_24h >= 5, pv.vwap_24h_raw,
            pv.last_price_raw > 0, pv.last_price_raw,
            0
        ) > 0
        """

        logger.debug(f'Executing CONSOLIDATED swap aggregation from {temp_db}.chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)

            # Decode binary token addresses
            decoded_result = []
            for row in result:
                token_value = row['token']
                base_coin_value = row['latest_base_coin']
                quote_coin_value = row['latest_quote_coin']

                # Decode token
                if isinstance(token_value, bytes):
                    token_str = token_value.decode('utf-8').rstrip('\x00')
                else:
                    token_str = str(token_value).rstrip('\x00')

                # Decode base_coin
                if isinstance(base_coin_value, bytes):
                    base_coin_str = base_coin_value.decode('utf-8').rstrip('\x00')
                else:
                    base_coin_str = str(base_coin_value).rstrip('\x00')

                # Decode quote_coin
                if isinstance(quote_coin_value, bytes):
                    quote_coin_str = quote_coin_value.decode('utf-8').rstrip('\x00')
                else:
                    quote_coin_str = str(quote_coin_value).rstrip('\x00')

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

    def _get_pools_for_chunk(self) -> List[Dict]:
        """
        Query pool data for tokens in temp database chunk_tokens table.
        Filters for pools where base_coin OR quote_coin is in the temp database chunk_tokens table.
        """
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE

        query = f"""
        SELECT
            CASE
                WHEN source LIKE 'jupiter6_%' THEN substring(source, 10)
                WHEN source LIKE 'jupiter4_%' THEN substring(source, 10)
                WHEN source LIKE 'raydium_route_%' THEN substring(source, 15)
                ELSE source
            END AS canonical_source,
            base_coin,
            quote_coin,
            argMax(base_pool_balance_after, block_time) AS last_base_balance,
            argMax(quote_pool_balance_after, block_time) AS last_quote_balance
        FROM solana.swaps
        WHERE
            (
                (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}'))
                OR
                (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}'))
            )
            AND
            (base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens) OR quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens))
        GROUP BY canonical_source, base_coin, quote_coin
        HAVING last_base_balance > 0 AND last_quote_balance > 0
        """

        logger.debug(f'Executing pool aggregation from {temp_db}.chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)
            # Decode binary token addresses to strings
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
                    'last_quote_balance': row['last_quote_balance']
                })
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get pool metrics: {e}', exc_info=True)
            return []

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
