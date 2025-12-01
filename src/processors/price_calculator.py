import logging
from typing import List, Dict
import polars as pl
from ..database import ClickHouseClient
from ..config import Config
logger = logging.getLogger(__name__)

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_DECIMALS = 9
STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}
STABLECOIN_DECIMALS = 6


class PriceCalculator:
    """
    Price calculator using VWAP (Volume-Weighted Average Price) from trade data.
    Receives pre-calculated VWAP prices from LiquidityAnalyzer and converts to USD.
    """

    def __init__(self, db_client: ClickHouseClient):
        self.db_client = db_client
        self.sol_price_usd = None  # Must be set via set_sol_price() before use

    def get_prices_for_chunk(self, price_data: List[Dict] = None, decimals_map: Dict[str, int] | None = None) -> pl.DataFrame:
        """
        Process VWAP price data from consolidated swap query.
        Converts raw VWAP prices to USD based on reference asset (SOL or stablecoin).

        Args:
            price_data: List of VWAP price data from LiquidityAnalyzer
            decimals_map: Token decimals for normalization

        Returns:
            Polars DataFrame with columns: mint, price_in_sol, price_usd, price_method
        """
        logger.info('Processing VWAP prices from consolidated swap query')

        price_data = price_data if price_data else []
        logger.info(f'Using {len(price_data)} VWAP price records')

        if not price_data:
            return pl.DataFrame({
                'mint': [],
                'price_in_sol': [],
                'price_usd': [],
                'price_method': []
            }, schema={
                'mint': pl.Utf8,
                'price_in_sol': pl.Float64,
                'price_usd': pl.Float64,
                'price_method': pl.Utf8
            })

        # Build results directly - VWAP is already calculated
        results = []
        stable_addresses = set(STABLECOINS.values())

        for row in price_data:
            token = row['token']
            price_raw = row.get('price_raw', 0)
            price_reference_type = row.get('price_reference_type', 'STABLE')
            price_method = row.get('price_method', 'UNKNOWN')

            if price_raw <= 0:
                continue

            # Get token decimals
            token_decimals = decimals_map.get(token, 6) if decimals_map else 6

            # Determine reference decimals based on type
            if price_reference_type == 'SOL':
                ref_decimals = SOL_DECIMALS
            else:
                ref_decimals = STABLECOIN_DECIMALS

            # Convert raw VWAP to normalized price
            # price_raw = sum(ref_amount) / sum(token_amount) in raw units
            # Normalized price = price_raw * 10^(token_decimals - ref_decimals)
            price_per_reference = price_raw * (10 ** (token_decimals - ref_decimals))

            # Convert to USD
            if price_reference_type == 'SOL':
                price_in_sol = price_per_reference
                price_usd = price_in_sol * self.sol_price_usd if self.sol_price_usd else None
            else:
                # Stablecoin - price is already in USD
                price_in_sol = price_per_reference / self.sol_price_usd if self.sol_price_usd else None
                price_usd = price_per_reference

            results.append({
                'mint': token,
                'price_in_sol': price_in_sol,
                'price_usd': price_usd,
                'price_method': price_method
            })

        df_prices = pl.DataFrame(results, schema={
            'mint': pl.Utf8,
            'price_in_sol': pl.Float64,
            'price_usd': pl.Float64,
            'price_method': pl.Utf8
        })

        # Log price method distribution
        if df_prices.height > 0:
            method_counts = df_prices.group_by('price_method').count()
            for row in method_counts.iter_rows():
                logger.debug(f'  {row[0]}: {row[1]} tokens')

        logger.info(f'VWAP prices processed for {len(df_prices)} tokens')
        return df_prices

    def get_sol_price(self) -> float:
        """Return current SOL price in USD."""
        return self.sol_price_usd

    def set_sol_price(self, price: float):
        """Update SOL price for calculations."""
        self.sol_price_usd = price
        logger.debug(f'SOL price set to ${price:.2f}')
