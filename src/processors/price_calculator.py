import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
from ..config import Config
logger = logging.getLogger(__name__)

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_PRICE_USD = 190.0
SOL_DECIMALS = 9
STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}
STABLECOIN_DECIMALS = 6

class PriceCalculator:
    """
    Chunk-optimized price calculator.
    Makes exactly 1 database query per chunk to get latest prices.
    Uses WHERE IN clause to filter for specific token chunk.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = SOL_PRICE_USD

    def get_prices_for_chunk(self, price_data: List[Dict] = None, decimals_map: Dict[str, int] | None = None) -> pl.DataFrame:
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
                schema={'token': pl.Utf8, 'price_reference': pl.Utf8, 'raw_price': pl.Float64}
            )
        else:
            df_prices = pl.DataFrame(
                {'token': [], 'price_reference': [], 'raw_price': []},
                schema={'token': pl.Utf8, 'price_reference': pl.Utf8, 'raw_price': pl.Float64}
            )

        # Rename token column to mint for consistency (always, even if empty)
        df_prices = df_prices.rename({'token': 'mint'})

        # Attach token decimals from RPC results
        if decimals_map:
            df_decimals = pl.DataFrame(
                {'mint': list(decimals_map.keys()), 'decimals': list(decimals_map.values())},
                schema={'mint': pl.Utf8, 'decimals': pl.Float64}
            )
        else:
            df_decimals = pl.DataFrame({'mint': [], 'decimals': []}, schema={'mint': pl.Utf8, 'decimals': pl.Float64})

        if df_decimals.height > 0:
            df_prices = df_prices.join(df_decimals, on='mint', how='left')
        else:
            df_prices = df_prices.with_columns(pl.lit(None).cast(pl.Float64).alias('decimals'))

        df_prices = df_prices.with_columns([
            pl.col('decimals').alias('token_decimals')
        ])

        # Reference coin decimals (SOL or stablecoin)
        stable_values = list(STABLECOINS.values())
        df_prices = df_prices.with_columns([
            pl.when(pl.col('price_reference') == SOL_ADDRESS)
            .then(float(SOL_DECIMALS))
            .when(pl.col('price_reference').is_in(stable_values))
            .then(float(STABLECOIN_DECIMALS))
            .otherwise(None)
            .alias('reference_decimals')
        ])

        # Adjust raw price using token/reference decimals; missing decimals -> null price
        df_prices = df_prices.with_columns([
            pl.when(
                pl.col('raw_price').is_not_null()
                & pl.col('token_decimals').is_not_null()
                & pl.col('reference_decimals').is_not_null()
            )
            .then(
                pl.col('raw_price') * pl.pow(10.0, pl.col('token_decimals') - pl.col('reference_decimals'))
            )
            .otherwise(None)
            .alias('price_per_reference')
        ])

        # Derive price_in_sol and price_usd depending on reference asset
        df_prices = df_prices.with_columns([
            pl.when(pl.col('price_reference') == SOL_ADDRESS)
            .then(pl.col('price_per_reference'))
            .otherwise(None)
            .alias('price_in_sol')
        ])

        df_prices = df_prices.with_columns([
            pl.when(pl.col('price_reference') == SOL_ADDRESS)
            .then(pl.col('price_in_sol') * SOL_PRICE_USD)
            .when(pl.col('price_reference').is_in(stable_values))
            .then(pl.col('price_per_reference'))
            .otherwise(None)
            .alias('price_usd')
        ])

        df_prices = df_prices.select(['mint', 'price_in_sol', 'price_usd'])

        logger.info(f'Prices fetched and processed for {len(df_prices)} tokens')
        return df_prices

    def get_sol_price(self) -> float:
        """Return current SOL price in USD."""
        return self.sol_price_usd

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
