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
        Get first transaction dates for tokens using mint data + provided swap data.
        Swap data now comes from the consolidated swap query in LiquidityAnalyzer.

        Args:
            first_swaps_data: Optional list of first swap data from consolidated query

        Returns:
            Polars DataFrame with columns: mint, first_tx_date
        """
        logger.info('Fetching first tx dates (mints from DB + swaps from consolidated query)')

        # Query: Get first mint date for this chunk (ONLY query to DB)
        first_mints = self._get_first_mints_for_chunk()
        logger.info(f'Retrieved {len(first_mints)} first mint records')

        # Use provided swap data (or empty list)
        first_swaps = first_swaps_data if first_swaps_data else []
        logger.info(f'Using {len(first_swaps)} first swap records from consolidated query')

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

        # Rename token column to mint for consistency (always, even if empty)
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

