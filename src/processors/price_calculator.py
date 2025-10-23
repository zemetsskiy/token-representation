import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
from ..config import Config
logger = logging.getLogger(__name__)

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_PRICE_USD = 190.0
STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}

class PriceCalculator:
    """
    Chunk-optimized price calculator.
    Makes exactly 1 database query per chunk to get latest prices.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_prices_for_chunk(self, price_data: List[Dict] = None) -> pl.DataFrame:
        """
        Process price data (provided from consolidated swap query).
        No longer queries the database - uses pre-fetched data.

        Args:
            price_data: List of price data from consolidated query

        Returns:
            Polars DataFrame with columns: mint, price_in_sol, price_usd
        """
        logger.info('Processing prices from consolidated swap query')

        # Use provided data (or empty list)
        price_data = price_data if price_data else []
        logger.info(f'Using {len(price_data)} price records from consolidated query')

        # Convert to Polars DataFrame with explicit schema
        if price_data:
            df_prices = pl.DataFrame(
                price_data,
                schema={'token': pl.Utf8, 'last_price_in_sol': pl.Float64}
            )
        else:
            df_prices = pl.DataFrame(
                {'token': [], 'last_price_in_sol': []},
                schema={'token': pl.Utf8, 'last_price_in_sol': pl.Float64}
            )

        # Rename token column to mint for consistency
        if len(df_prices) > 0:
            df_prices = df_prices.rename({'token': 'mint', 'last_price_in_sol': 'price_in_sol'})

        # Fill nulls with 0 and calculate USD price
        df_prices = df_prices.with_columns([
            pl.col('price_in_sol').fill_null(0),
            (pl.col('price_in_sol').fill_null(0) * SOL_PRICE_USD).alias('price_usd')
        ])

        logger.info(f'Prices fetched and processed for {len(df_prices)} tokens')
        return df_prices

    def get_sol_price(self) -> float:
        """Return current SOL price in USD."""
        return self.sol_price_usd

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
