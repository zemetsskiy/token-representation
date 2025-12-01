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
        This consolidates: first_swap dates, pool metrics, and price data.

        Returns:
            Dict with keys:
                - 'pool_data': List of pool metrics
                - 'first_swaps': List of first swap dates
                - 'prices': List of price data
        """
        logger.info('Fetching comprehensive swap data from chunk_tokens table (1 CONSOLIDATED query)')

        comprehensive_data = self._get_comprehensive_swap_data()

        logger.info(f'Comprehensive swap query completed: {len(comprehensive_data)} token records')

        # Separate data into different categories for downstream processing
        pool_data = []
        first_swaps = []
        prices = []

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
                    'last_quote_balance': row['latest_quote_balance']
                })

            # Price data (now based on Deepest Pool Reserves)
            if row.get('latest_price_reference'):
                prices.append({
                    'token': token,
                    'price_reference': row['latest_price_reference'],
                    # Pass reserves for calculation
                    'base_coin': row['latest_base_coin'],
                    'quote_coin': row['latest_quote_coin'],
                    'base_balance': row['latest_base_balance'],
                    'quote_balance': row['latest_quote_balance']
                })

        logger.info(f'Extracted: {len(first_swaps)} first swaps, {len(pool_data)} pools, {len(prices)} prices')

        return {
            'pool_data': pool_data,
            'first_swaps': first_swaps,
            'prices': prices
        }

    def _get_comprehensive_swap_data(self) -> List[Dict]:
        """
        CONSOLIDATED QUERY: Get ALL swap data (first_swap, pools, prices) in ONE query.
        Uses ARRAY JOIN for maximum performance instead of UNION ALL.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        usdc = STABLECOINS['USDC']
        usdt = STABLECOINS['USDT']

        # ONLY include direct DEX sources with accurate pool balances
        # Exclude all aggregators (jupiter*, raydium_route*) as they don't have real per-pool balances
        allowed_sources = [
            # Direct DEX sources only
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
        -- 1. Unify base and quote side swaps into a single stream of "Token + Pool" events
        -- IMPORTANT: Exclude multi-hop/routing sources as they don't have real pool balances
        unified_swaps AS (
            SELECT
                base_coin AS token,
                source,
                base_coin,
                quote_coin,
                base_pool_balance_after,
                quote_pool_balance_after,
                block_time,
                -- Identify the reference asset (money) in this pair
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN 'STABLE'
                    WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN 'STABLE'
                    ELSE 'OTHER'
                END as ref_type,
                -- Get the raw balance of the reference asset
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN quote_pool_balance_after
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN quote_pool_balance_after
                    WHEN base_coin = '{SOL_ADDRESS}' THEN base_pool_balance_after
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN base_pool_balance_after
                    ELSE 0
                END as ref_balance_raw
            FROM solana.swaps
            PREWHERE
                base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
                AND (
                    quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}')
                    OR base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}')
                )
            WHERE source IN ({allowed_sources_sql})

            UNION ALL

            SELECT
                quote_coin AS token,
                source,
                base_coin,
                quote_coin,
                base_pool_balance_after,
                quote_pool_balance_after,
                block_time,
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN 'STABLE'
                    WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN 'STABLE'
                    ELSE 'OTHER'
                END as ref_type,
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN quote_pool_balance_after
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN quote_pool_balance_after
                    WHEN base_coin = '{SOL_ADDRESS}' THEN base_pool_balance_after
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN base_pool_balance_after
                    ELSE 0
                END as ref_balance_raw
            FROM solana.swaps
            PREWHERE
                quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
                AND (
                    quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}')
                    OR base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}')
                )
            WHERE source IN ({allowed_sources_sql})
        ),
        
        -- 2. Aggregate per POOL to find the latest state and approximate liquidity
        pool_stats AS (
            SELECT
                token,
                -- Canonical source name cleanup
                CASE
                    WHEN source LIKE 'jupiter6_%' THEN substring(source, 10)
                    WHEN source LIKE 'jupiter4_%' THEN substring(source, 10)
                    WHEN source LIKE 'raydium_route_%' THEN substring(source, 15)
                    ELSE source
                END AS canonical_source,
                base_coin,
                quote_coin,
                -- Latest state of this pool
                argMax(base_pool_balance_after, block_time) as latest_base_bal,
                argMax(quote_pool_balance_after, block_time) as latest_quote_bal,
                min(block_time) as first_swap_time,
                -- Liquidity Score Calculation (Approximate USD value of the Reference Side)
                -- We use this ONLY for ranking pools, so exact precision isn't critical, but order is.
                -- SOL = 9 decimals, price from Redis
                -- Stable = 6 decimals, Price $1
                argMax(
                    CASE
                        WHEN ref_type = 'SOL' THEN (ref_balance_raw / 1e9) * {self.sol_price_usd}
                        WHEN ref_type = 'STABLE' THEN (ref_balance_raw / 1e6)
                        ELSE 0
                    END,
                    block_time
                ) as liquidity_score_usd
            FROM unified_swaps
            GROUP BY token, canonical_source, base_coin, quote_coin
        )

        -- 3. Select the BEST pool for each token (Max Liquidity)
        SELECT
            token,
            min(first_swap_time) as first_swap,
            
            -- Info from the Deepest Pool
            argMax(canonical_source, liquidity_score_usd) as latest_source,
            argMax(base_coin, liquidity_score_usd) as latest_base_coin,
            argMax(quote_coin, liquidity_score_usd) as latest_quote_coin,
            argMax(latest_base_bal, liquidity_score_usd) as latest_base_balance,
            argMax(latest_quote_bal, liquidity_score_usd) as latest_quote_balance,
            
            -- We NO LONGER return a pre-calculated price. 
            -- We return the raw reserves of the best pool.
            -- The PriceCalculator will compute price = quote / base.
            
            -- For backward compatibility with the return dict, we pass the reference coin
            argMax(
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN '{SOL_ADDRESS}'
                    WHEN base_coin = '{SOL_ADDRESS}' THEN '{SOL_ADDRESS}'
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN quote_coin
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN base_coin
                    ELSE NULL
                END,
                liquidity_score_usd
            ) as latest_price_reference,
            
            -- Pass 0 as raw price, we will calculate it in Python
            0.0 as latest_price_raw
            
        FROM pool_stats
        GROUP BY token
        HAVING latest_base_balance > 0 AND latest_quote_balance > 0
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
                    'latest_price_raw': row['latest_price_raw'],
                    'latest_price_reference': price_reference_str
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
