import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
from ..config import Config
logger = logging.getLogger(__name__)

class SupplyCalculator:
    """
    Chunk-optimized supply calculator.
    Makes exactly 2 database queries per chunk (minted + burned aggregates).
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def get_supplies_for_chunk(self) -> pl.DataFrame:
        """
        Get supply data for tokens in the temporary 'chunk_tokens' table using exactly 2 queries.
        Uses WHERE mint IN (SELECT mint FROM chunk_tokens) to filter results.

        Returns:
            Polars DataFrame with columns: mint, total_minted, total_burned
        """
        logger.info('Fetching supply data from chunk_tokens table (2 batch queries)')

        # Query 1: Get minted amounts for this chunk
        minted_data = self._get_minted_for_chunk()
        logger.info(f'Query 1/2: Retrieved {len(minted_data)} minted records')

        # Query 2: Get burned amounts for this chunk
        burned_data = self._get_burned_for_chunk()
        logger.info(f'Query 2/2: Retrieved {len(burned_data)} burned records')

        # Convert to Polars DataFrames with explicit schema to handle large integers
        if minted_data:
            df_minted = pl.DataFrame(
                minted_data,
                schema={'mint': pl.Utf8, 'total_minted': pl.UInt64}
            )
        else:
            df_minted = pl.DataFrame({'mint': [], 'total_minted': []}, schema={'mint': pl.Utf8, 'total_minted': pl.UInt64})

        if burned_data:
            df_burned = pl.DataFrame(
                burned_data,
                schema={'mint': pl.Utf8, 'total_burned': pl.UInt64}
            )
        else:
            df_burned = pl.DataFrame({'mint': [], 'total_burned': []}, schema={'mint': pl.Utf8, 'total_burned': pl.UInt64})

        # Join minted and burned data
        df_chunk = df_minted.join(df_burned, on='mint', how='outer')

        # Fill nulls with 0
        df_chunk = df_chunk.with_columns([
            pl.col('total_minted').fill_null(0),
            pl.col('total_burned').fill_null(0)
        ])

        logger.info(f'Supply data fetched and processed for {len(df_chunk)} tokens')
        return df_chunk

    def _get_minted_for_chunk(self) -> List[Dict]:
        """
        Query minted amounts for tokens in temp database chunk_tokens table.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        query = f"""
        SELECT
            mint,
            SUM(amount) AS total_minted
        FROM solana.mints
        WHERE mint IN (SELECT mint FROM {temp_db}.chunk_tokens)
        GROUP BY mint
        """

        logger.debug(f'Executing minted aggregation from {temp_db}.chunk_tokens table')
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
                decoded_result.append({'mint': mint_str, 'total_minted': row['total_minted']})
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get minted amounts: {e}', exc_info=True)
            return []

    def _get_burned_for_chunk(self) -> List[Dict]:
        """
        Query burned amounts for tokens in temp database chunk_tokens table.
        """
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        query = f"""
        SELECT
            mint,
            SUM(amount) AS total_burned
        FROM solana.burns
        WHERE mint IN (SELECT mint FROM {temp_db}.chunk_tokens)
        GROUP BY mint
        """

        logger.debug(f'Executing burned aggregation from {temp_db}.chunk_tokens table')
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
                decoded_result.append({'mint': mint_str, 'total_burned': row['total_burned']})
            return decoded_result
        except Exception as e:
            logger.error(f'Failed to get burned amounts: {e}', exc_info=True)
            return []
