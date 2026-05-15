import logging
from typing import List, Dict
import polars as pl
from ...database import ClickHouseClient
from ...config import Config
logger = logging.getLogger(__name__)

class FirstTxFinder:
    """
    Chunk-optimized first transaction finder.
    Makes exactly 2 database queries per chunk (first mints + first swaps).
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def get_first_tx_for_chunk(self, first_swaps_data: List[Dict] = None) -> pl.DataFrame:
        """
        Get first transaction dates for tokens using mint data + swap data.
        Three sources combined:
          1. solana.mints table (SPL Token mint transactions)
          2. Consolidated query first_swap (last 7 days, SOL/USDC/USDT pairs only)
          3. Direct swaps query fallback (all-time, any pair — catches Token-2022 and LST pairs)

        Args:
            first_swaps_data: Optional list of first swap data from consolidated query

        Returns:
            Polars DataFrame with columns: mint, first_tx_date
        """
        logger.info('Fetching first tx dates (mints + consolidated swaps + direct swaps fallback)')

        # Source 1: First mint date from solana.mints
        first_mints = self._get_first_mints_for_chunk()
        logger.info(f'Retrieved {len(first_mints)} first mint records')

        # Source 2: First swap from consolidated query (7-day window, SOL/STABLE pairs)
        first_swaps = first_swaps_data if first_swaps_data else []
        logger.info(f'Using {len(first_swaps)} first swap records from consolidated query')

        # Source 3: Direct first swap from swaps table (all-time, any pair)
        first_swaps_direct = self._get_first_swaps_for_chunk()
        logger.info(f'Retrieved {len(first_swaps_direct)} first swap records from direct query')

        # Build DataFrames
        df_first_mints = pl.DataFrame(
            first_mints if first_mints else {'mint': [], 'first_mint': []},
            schema={'mint': pl.Utf8, 'first_mint': pl.Datetime}
        )

        df_first_swaps = pl.DataFrame(
            first_swaps if first_swaps else {'token': [], 'first_swap': []},
            schema={'token': pl.Utf8, 'first_swap': pl.Datetime}
        ).rename({'token': 'mint'})

        df_first_swaps_direct = pl.DataFrame(
            first_swaps_direct if first_swaps_direct else {'mint': [], 'first_swap_direct': []},
            schema={'mint': pl.Utf8, 'first_swap_direct': pl.Datetime}
        )

        # Join all three sources — coalesce mint after each outer join
        df_chunk = df_first_mints.join(df_first_swaps, on='mint', how='outer', coalesce=True)
        df_chunk = df_chunk.join(df_first_swaps_direct, on='mint', how='outer', coalesce=True)

        # Calculate earliest date across all sources
        df_chunk = df_chunk.with_columns([
            pl.min_horizontal(['first_mint', 'first_swap', 'first_swap_direct']).alias('first_tx_date')
        ])

        df_chunk = df_chunk.select(['mint', 'first_tx_date'])

        logger.info(f'First tx dates fetched and processed for {len(df_chunk)} tokens')
        return df_chunk

    def _get_first_swaps_for_chunk(self) -> List[Dict]:
        """
        Query first swap dates for tokens in chunk_tokens table.
        Scans ALL time and ANY pair (not limited to SOL/USDC/USDT).
        Catches Token-2022 tokens missing from solana.mints and tokens
        trading only against LSTs or other non-standard pairs.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        query = f"""
        SELECT token, MIN(first_swap) AS first_swap_direct
        FROM (
            SELECT base_coin AS token, MIN(block_time) AS first_swap
            FROM solana.swaps
            WHERE base_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
            GROUP BY base_coin
            UNION ALL
            SELECT quote_coin AS token, MIN(block_time) AS first_swap
            FROM solana.swaps
            WHERE quote_coin IN (SELECT mint FROM {temp_db}.chunk_tokens)
            GROUP BY quote_coin
        )
        GROUP BY token
        """

        try:
            result = self.db_client.execute_query_dict(query)
            decoded = []
            for row in result:
                mint_value = row['token']
                if isinstance(mint_value, bytes):
                    mint_str = mint_value.decode('utf-8').rstrip('\x00')
                else:
                    mint_str = str(mint_value).rstrip('\x00')
                decoded.append({'mint': mint_str, 'first_swap_direct': row['first_swap_direct']})
            return decoded
        except Exception as e:
            logger.error(f'Failed to get first swaps (direct): {e}', exc_info=True)
            return []

    def _get_first_mints_for_chunk(self) -> List[Dict]:
        """
        Query first mint dates for tokens in temp database chunk_tokens table.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        query = f"""
        SELECT
            mint,
            MIN(block_time) as first_mint
        FROM solana.mints
        WHERE mint IN (SELECT mint FROM {temp_db}.chunk_tokens)
        GROUP BY mint
        """

        logger.debug(f'Executing first mint aggregation from {temp_db}.chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)
            # Decode binary mint addresses to strings
            decoded_result = []
            for row in result:
                mint_value = row['mint']
                if isinstance(mint_value, bytes):
                    mint_str = mint_value.decode('utf-8').rstrip('\x00')
                else:
                    mint_str = str(mint_value).rstrip('\x00')
                decoded_result.append({'mint': mint_str, 'first_mint': row['first_mint']})
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get first mints: {e}', exc_info=True)
            return []

