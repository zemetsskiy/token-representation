import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
logger = logging.getLogger(__name__)

class SupplyCalculator:
    """
    Chunk-optimized supply calculator.
    Makes exactly 2 database queries per chunk (minted + burned aggregates).
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client

    def get_supplies_for_chunk(self, token_addresses: List[str]) -> pl.DataFrame:
        """
        Get supply data for a SPECIFIC CHUNK of tokens using exactly 2 queries.
        Uses WHERE mint IN (...) to filter results.

        Args:
            token_addresses: List of token mint addresses to process

        Returns:
            Polars DataFrame with columns: mint, total_minted, total_burned
        """
        if not token_addresses:
            return pl.DataFrame({'mint': [], 'total_minted': [], 'total_burned': []})

        logger.info(f'Fetching supply data for {len(token_addresses)} tokens (2 batch queries)')

        # Query 1: Get minted amounts for this chunk
        minted_data = self._get_minted_for_chunk(token_addresses)
        logger.info(f'Query 1/2: Retrieved {len(minted_data)} minted records')

        # Query 2: Get burned amounts for this chunk
        burned_data = self._get_burned_for_chunk(token_addresses)
        logger.info(f'Query 2/2: Retrieved {len(burned_data)} burned records')

        # Convert to Polars DataFrames
        df_minted = pl.DataFrame(minted_data) if minted_data else pl.DataFrame({'mint': [], 'total_minted': []})
        df_burned = pl.DataFrame(burned_data) if burned_data else pl.DataFrame({'mint': [], 'total_burned': []})

        # Create base DataFrame with all tokens in chunk
        df_chunk = pl.DataFrame({'mint': token_addresses})

        # Join minted and burned data
        df_chunk = df_chunk.join(df_minted, on='mint', how='left')
        df_chunk = df_chunk.join(df_burned, on='mint', how='left')

        # Fill nulls with 0
        df_chunk = df_chunk.with_columns([
            pl.col('total_minted').fill_null(0),
            pl.col('total_burned').fill_null(0)
        ])

        logger.info(f'Supply data fetched and processed for {len(df_chunk)} tokens')
        return df_chunk

    def _get_minted_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Query minted amounts for specific tokens using WHERE IN clause.
        """
        # Build WHERE IN clause
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])

        query = f"""
        SELECT
            mint,
            SUM(amount) AS total_minted
        FROM solana.mints
        WHERE mint IN ({placeholders})
        GROUP BY mint
        """

        logger.debug(f'Executing minted aggregation for {len(token_addresses)} tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get minted amounts: {e}', exc_info=True)
            return []

    def _get_burned_for_chunk(self, token_addresses: List[str]) -> List[Dict]:
        """
        Query burned amounts for specific tokens using WHERE IN clause.
        """
        # Build WHERE IN clause
        placeholders = ', '.join([f"'{t}'" for t in token_addresses])

        query = f"""
        SELECT
            mint,
            SUM(amount) AS total_burned
        FROM solana.burns
        WHERE mint IN ({placeholders})
        GROUP BY mint
        """

        logger.debug(f'Executing burned aggregation for {len(token_addresses)} tokens')
        try:
            result = self.db_client.execute_query_dict(query)
            return result
        except Exception as e:
            logger.error(f'Failed to get burned amounts: {e}', exc_info=True)
            return []
