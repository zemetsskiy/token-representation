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

    def get_first_tx_for_chunk(self) -> pl.DataFrame:
        """
        Get first transaction dates for tokens in chunk_tokens temporary table using exactly 2 queries.
        Uses WHERE IN (SELECT mint FROM chunk_tokens) to filter results.

        Returns:
            Polars DataFrame with columns: mint, first_tx_date
        """
        logger.info('Fetching first tx dates from chunk_tokens table (2 batch queries)')

        # Query 1: Get first mint date for this chunk
        first_mints = self._get_first_mints_for_chunk()
        logger.info(f'Query 1/2: Retrieved {len(first_mints)} first mint records')

        # Query 2: Get first swap date for this chunk
        first_swaps = self._get_first_swaps_for_chunk()
        logger.info(f'Query 2/2: Retrieved {len(first_swaps)} first swap records')

        # Convert to Polars DataFrames with explicit schema
        if first_mints:
            df_first_mints = pl.DataFrame(
                first_mints,
                schema={'mint': pl.Utf8, 'first_mint': pl.Datetime}
            )
        else:
            df_first_mints = pl.DataFrame(
                {'mint': [], 'first_mint': []},
                schema={'mint': pl.Utf8, 'first_mint': pl.Datetime}
            )

        if first_swaps:
            df_first_swaps = pl.DataFrame(
                first_swaps,
                schema={'token': pl.Utf8, 'first_swap': pl.Datetime}
            )
        else:
            df_first_swaps = pl.DataFrame(
                {'token': [], 'first_swap': []},
                schema={'token': pl.Utf8, 'first_swap': pl.Datetime}
            )

        # Rename token column to mint for consistency
        if len(df_first_swaps) > 0:
            df_first_swaps = df_first_swaps.rename({'token': 'mint'})

        # Join first tx dates
        df_chunk = df_first_mints.join(df_first_swaps, on='mint', how='outer')

        # Calculate earliest date between mint and swap
        df_chunk = df_chunk.with_columns([
            pl.min_horizontal(['first_mint', 'first_swap']).alias('first_tx_date')
        ])

        # Keep only mint and first_tx_date columns
        df_chunk = df_chunk.select(['mint', 'first_tx_date'])

        logger.info(f'First tx dates fetched and processed for {len(df_chunk)} tokens')
        return df_chunk

    def _get_first_mints_for_chunk(self) -> List[Dict]:
        """
        Query first mint dates for tokens in chunk_tokens temporary table.
        """
        query = """
        SELECT
            mint,
            MIN(block_time) as first_mint
        FROM solana.mints
        WHERE mint IN (SELECT mint FROM chunk_tokens)
        GROUP BY mint
        """

        logger.debug('Executing first mint aggregation from chunk_tokens table')
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

    def _get_first_swaps_for_chunk(self) -> List[Dict]:
        """
        Query first swap dates for tokens in chunk_tokens temporary table.
        Uses UNION ALL to check both base_coin and quote_coin.
        """
        query = """
        SELECT
            token,
            MIN(block_time) as first_swap
        FROM (
            SELECT base_coin as token, block_time
            FROM solana.swaps
            WHERE base_coin IN (SELECT mint FROM chunk_tokens)

            UNION ALL

            SELECT quote_coin as token, block_time
            FROM solana.swaps
            WHERE quote_coin IN (SELECT mint FROM chunk_tokens)
        )
        GROUP BY token
        """

        logger.debug('Executing first swap aggregation from chunk_tokens table')
        try:
            result = self.db_client.execute_query_dict(query)
            # Decode binary token addresses to strings
            decoded_result = []
            for row in result:
                token_value = row['token']
                if isinstance(token_value, bytes):
                    token_str = token_value.decode('utf-8').rstrip('\x00')
                else:
                    token_str = str(token_value).rstrip('\x00')
                decoded_result.append({'token': token_str, 'first_swap': row['first_swap']})
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get first swaps: {e}', exc_info=True)
            return []
