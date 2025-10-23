import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class FirstTxFinder:
    """
    Chunk-optimized first transaction finder.
    Makes exactly 2 database queries per chunk (first mints + first swaps).
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def get_first_tx_for_chunk(self, token_addresses: List[str]) -> pl.DataFrame:
        """
        Get first transaction dates for a SPECIFIC CHUNK of tokens using exactly 2 queries.
        Uses WHERE IN (...) to filter results.

        Args:
            token_addresses: List of token mint addresses to process

        Returns:
            Polars DataFrame with columns: mint, first_tx_date
        """
        if not token_addresses:
            return pl.DataFrame({'mint': [], 'first_tx_date': []})

        logger.info(f'Fetching first tx dates for {len(token_addresses)} tokens (2 batch queries)')

        # Query 1: Get first mint date for this chunk
        first_mints = self._get_first_mints_for_chunk(token_addresses)
        logger.info(f'Query 1/2: Retrieved {len(first_mints)} first mint records')

        # Query 2: Get first swap date for this chunk
        first_swaps = self._get_first_swaps_for_chunk(token_addresses)
        logger.info(f'Query 2/2: Retrieved {len(first_swaps)} first swap records')

        # Convert to Polars DataFrames
        df_first_mints = pl.DataFrame(first_mints) if first_mints else pl.DataFrame({'mint': [], 'first_mint': []})
        df_first_swaps = pl.DataFrame(first_swaps) if first_swaps else pl.DataFrame({'token': [], 'first_swap': []})

        # Rename token column to mint for consistency
        if len(df_first_swaps) > 0:
            df_first_swaps = df_first_swaps.rename({'token': 'mint'})

        # Create base DataFrame with all tokens in chunk
        df_chunk = pl.DataFrame({'mint': token_addresses})

        # Join first tx dates
        df_chunk = df_chunk.join(df_first_mints, on='mint', how='left')
        df_chunk = df_chunk.join(df_first_swaps, on='mint', how='left')

        # Calculate earliest date between mint and swap
        df_chunk = df_chunk.with_columns([
            pl.min_horizontal(['first_mint', 'first_swap']).alias('first_tx_date')
        ])

        # Keep only mint and first_tx_date columns
        df_chunk = df_chunk.select(['mint', 'first_tx_date'])

        logger.info(f'First tx dates fetched and processed for {len(df_chunk)} tokens')
        return df_chunk

    def _get_first_mints_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Query first mint dates for specific tokens using WHERE IN clause.
        """
        # Build WHERE IN clause
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])

        query = f"""
        SELECT
            mint,
            MIN(block_time) as first_mint
        FROM solana.mints
        WHERE mint IN ({placeholders})
        GROUP BY mint
        """

        logger.debug(f'Executing first mint aggregation for {len(token_addresses)} tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get first mints: {e}', exc_info=True)
            return []

    def _get_first_swaps_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Query first swap dates for specific tokens using WHERE IN clause.
        Uses UNION ALL to check both base_coin and quote_coin.
        """
        # Build WHERE IN clause
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])

        query = f"""
        SELECT
            token,
            MIN(block_time) as first_swap
        FROM (
            SELECT base_coin as token, block_time
            FROM solana.swaps
            WHERE base_coin IN ({placeholders})

            UNION ALL

            SELECT quote_coin as token, block_time
            FROM solana.swaps
            WHERE quote_coin IN ({placeholders})
        )
        GROUP BY token
        """

        logger.debug(f'Executing first swap aggregation for {len(token_addresses)} tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get first swaps: {e}', exc_info=True)
            return []
