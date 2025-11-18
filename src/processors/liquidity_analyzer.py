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
SOL_PRICE_USD = 190.0

class LiquidityAnalyzer:
    """
    Chunk-optimized liquidity analyzer.
    Makes exactly 1 database query per chunk to get pool data.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

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

            # Price data (if available)
            if row.get('latest_price_raw') and row.get('latest_price_reference'):
                prices.append({
                    'token': token,
                    'price_reference': row['latest_price_reference'],
                    'raw_price': row['latest_price_raw']
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

        query = f"""
        WITH
        base_side AS (
            SELECT
                base_coin AS token,
                block_time,
                source,
                base_coin,
                quote_coin,
                base_coin_amount,
                quote_coin_amount,
                base_pool_balance_after,
                quote_pool_balance_after
            FROM solana.swaps
            PREWHERE
                base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
                AND (
                    quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}')
                    OR base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}')
                )
        ),
        quote_side AS (
            SELECT
                quote_coin AS token,
                block_time,
                source,
                base_coin,
                quote_coin,
                base_coin_amount,
                quote_coin_amount,
                base_pool_balance_after,
                quote_pool_balance_after
            FROM solana.swaps
            PREWHERE
                quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
                AND (
                    quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{usdc}', '{usdt}')
                    OR base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{usdc}', '{usdt}')
                )
        )
        SELECT
            token,
            MIN(block_time) AS first_swap,
            argMax(
                CASE
                    WHEN source LIKE 'jupiter6_%' THEN substring(source, 10)
                    WHEN source LIKE 'jupiter4_%' THEN substring(source, 10)
                    WHEN source LIKE 'raydium_route_%' THEN substring(source, 15)
                    ELSE source
                END,
                block_time
            ) AS latest_source,
            argMax(base_coin, block_time) AS latest_base_coin,
            argMax(quote_coin, block_time) AS latest_quote_coin,
            argMax(base_pool_balance_after, block_time) AS latest_base_balance,
            argMax(quote_pool_balance_after, block_time) AS latest_quote_balance,
            argMax(
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN quote_coin_amount / NULLIF(base_coin_amount, 0)
                    WHEN base_coin = '{SOL_ADDRESS}' THEN base_coin_amount / NULLIF(quote_coin_amount, 0)
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN quote_coin_amount / NULLIF(base_coin_amount, 0)
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN base_coin_amount / NULLIF(quote_coin_amount, 0)
                    ELSE NULL
                END,
                block_time
            ) AS latest_price_raw,
            argMax(
                CASE
                    WHEN quote_coin = '{SOL_ADDRESS}' THEN '{SOL_ADDRESS}'
                    WHEN base_coin = '{SOL_ADDRESS}' THEN '{SOL_ADDRESS}'
                    WHEN quote_coin IN ('{usdc}', '{usdt}') THEN quote_coin
                    WHEN base_coin IN ('{usdc}', '{usdt}') THEN base_coin
                    ELSE NULL
                END,
                block_time
            ) AS latest_price_reference
        FROM (
            SELECT * FROM base_side
            UNION ALL
            SELECT * FROM quote_side
        )
        WHERE token IN (SELECT mint FROM {temp_db}.chunk_tokens)
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
