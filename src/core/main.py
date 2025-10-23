import logging
import time
from typing import Dict
import polars as pl
from ..config import Config, setup_logging
from ..database import get_db_client, ClickHouseClient
from ..processors import (
    TokenDiscovery,
    SupplyCalculator,
    PriceCalculator,
    LiquidityAnalyzer,
    FirstTxFinder
)

setup_logging()
logger = logging.getLogger(__name__)

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
SOL_PRICE_USD = 190.0
STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}


class TokenAggregationWorker:
    """
    High-performance token aggregation worker using Polars.
    Processes millions of tokens efficiently with batch queries and in-memory joins.
    """

    def __init__(self):
        logger.info('Initializing Token Aggregation Worker (Polars-optimized)')
        self.db_client = get_db_client()
        self.token_discovery = TokenDiscovery(self.db_client)
        self.supply_calculator = SupplyCalculator(self.db_client)
        self.price_calculator = PriceCalculator(self.db_client)
        self.liquidity_analyzer = LiquidityAnalyzer(self.db_client)
        self.first_tx_finder = FirstTxFinder(self.db_client)
        self.performance_metrics = {}
        logger.info('Worker initialized successfully')

    def process_all_tokens(self) -> int:
        """
        Process ALL tokens using batch queries and Polars for in-memory processing.
        NO per-token loops, NO LIMIT on token count.
        """
        total_start = time.time()
        logger.info('=' * 100)
        logger.info('STARTING FULL TOKEN AGGREGATION (NO LIMITS)')
        logger.info('=' * 100)

        try:
            # Step 1: Discover all token mints
            logger.info('Step 1/6: Discovering token mints')
            step_start = time.time()
            mints_data = self.token_discovery.discover_all_token_mints()
            df_tokens = pl.DataFrame(mints_data)
            self.performance_metrics['discover_mints'] = time.time() - step_start
            logger.info(f'Discovered {len(df_tokens)} tokens in {self.performance_metrics["discover_mints"]:.2f}s')

            if len(df_tokens) == 0:
                logger.warning('No tokens found')
                return 0

            # Step 2: Get supply data (minted + burned)
            logger.info('Step 2/6: Fetching supply data (2 queries)')
            step_start = time.time()
            supply_data = self.supply_calculator.get_all_supplies_batch()

            # Convert to Polars DataFrames
            df_minted = pl.DataFrame(supply_data['minted'])
            df_burned = pl.DataFrame(supply_data['burned'])

            # Join supply data
            df_tokens = df_tokens.join(df_minted, on='mint', how='left')
            df_tokens = df_tokens.join(df_burned, on='mint', how='left')

            # Fill nulls with 0
            df_tokens = df_tokens.with_columns([
                pl.col('total_minted').fill_null(0),
                pl.col('total_burned').fill_null(0)
            ])

            self.performance_metrics['fetch_supply'] = time.time() - step_start
            logger.info(f'Supply data fetched and joined in {self.performance_metrics["fetch_supply"]:.2f}s')

            # Step 3: Get first transaction dates
            logger.info('Step 3/6: Fetching first transaction dates (2 queries)')
            step_start = time.time()
            first_tx_data = self.first_tx_finder.get_all_first_tx_dates_batch()

            # Convert to Polars DataFrames
            df_first_mints = pl.DataFrame(first_tx_data['first_mints'])
            df_first_swaps = pl.DataFrame(first_tx_data['first_swaps'])

            # Join first tx dates
            df_tokens = df_tokens.join(df_first_mints, on='mint', how='left')
            df_tokens = df_tokens.join(
                df_first_swaps.rename({'token': 'mint'}),
                on='mint',
                how='left'
            )

            # Calculate earliest date between mint and swap
            df_tokens = df_tokens.with_columns([
                pl.min_horizontal(['first_mint', 'first_swap']).alias('first_tx_date')
            ])

            self.performance_metrics['fetch_first_tx'] = time.time() - step_start
            logger.info(f'First tx dates fetched and joined in {self.performance_metrics["fetch_first_tx"]:.2f}s')

            # Step 4: Get pool metrics (liquidity)
            logger.info('Step 4/6: Fetching pool metrics (1 query)')
            step_start = time.time()
            pool_data = self.liquidity_analyzer.get_all_pool_metrics_batch()
            df_pools = pl.DataFrame(pool_data)

            self.performance_metrics['fetch_pools'] = time.time() - step_start
            logger.info(f'Pool data fetched in {self.performance_metrics["fetch_pools"]:.2f}s')

            # Step 5: Get prices
            logger.info('Step 5/6: Fetching prices (1 query)')
            step_start = time.time()
            price_data = self.price_calculator.get_all_prices_batch()
            df_prices = pl.DataFrame(price_data).rename({'token': 'mint'})

            # Join prices
            df_tokens = df_tokens.join(df_prices, on='mint', how='left')

            # Convert price from SOL to USD
            df_tokens = df_tokens.with_columns([
                (pl.col('last_price_in_sol').fill_null(0) * SOL_PRICE_USD).alias('price_usd')
            ])

            self.performance_metrics['fetch_prices'] = time.time() - step_start
            logger.info(f'Prices fetched and converted in {self.performance_metrics["fetch_prices"]:.2f}s')

            # Step 6: Process pool data and calculate final metrics in Polars
            logger.info('Step 6/6: Processing pools and calculating metrics in memory')
            step_start = time.time()

            df_tokens = self._process_pools_and_metrics(df_tokens, df_pools)

            self.performance_metrics['process_polars'] = time.time() - step_start
            logger.info(f'In-memory processing completed in {self.performance_metrics["process_polars"]:.2f}s')

            # Calculate total time
            self.performance_metrics['total'] = time.time() - total_start

            # Print results
            self._print_results(df_tokens)
            self._print_performance_report()

            return len(df_tokens)

        except Exception as e:
            logger.error(f'Error processing tokens: {e}', exc_info=True)
            raise

    def _process_pools_and_metrics(self, df_tokens: pl.DataFrame, df_pools: pl.DataFrame) -> pl.DataFrame:
        """
        Process pool data and calculate liquidity + market cap metrics using Polars.
        All calculations happen in memory - NO database queries, NO loops.
        """
        logger.info('Calculating liquidity and market cap metrics in Polars')

        # Calculate liquidity_usd for each pool
        df_pools = df_pools.with_columns([
            pl.when(pl.col('base_coin') == SOL_ADDRESS)
            .then(pl.col('last_base_balance') * SOL_PRICE_USD * 2.0)
            .when(pl.col('quote_coin') == SOL_ADDRESS)
            .then(pl.col('last_quote_balance') * SOL_PRICE_USD * 2.0)
            .when(pl.col('base_coin').is_in(list(STABLECOINS.values())))
            .then(pl.col('last_base_balance') * 2.0)
            .when(pl.col('quote_coin').is_in(list(STABLECOINS.values())))
            .then(pl.col('last_quote_balance') * 2.0)
            .otherwise(0.0)
            .alias('liquidity_usd')
        ])

        # For each token, find the pool with highest liquidity
        # First, create a dataset where each token appears with its pools
        df_pools_base = df_pools.select([
            pl.col('base_coin').alias('mint'),
            pl.col('canonical_source').alias('source'),
            pl.col('liquidity_usd')
        ])

        df_pools_quote = df_pools.select([
            pl.col('quote_coin').alias('mint'),
            pl.col('canonical_source').alias('source'),
            pl.col('liquidity_usd')
        ])

        # Combine both
        df_token_pools = pl.concat([df_pools_base, df_pools_quote])

        # Get max liquidity per token
        df_best_pools = (
            df_token_pools
            .group_by('mint')
            .agg([
                pl.col('liquidity_usd').max().alias('largest_lp_pool_usd'),
                pl.col('source').first().alias('source')  # Could improve this to get source of max liquidity
            ])
        )

        # Join best pools to tokens
        df_tokens = df_tokens.join(df_best_pools, on='mint', how='left')

        # Fill nulls
        df_tokens = df_tokens.with_columns([
            pl.col('largest_lp_pool_usd').fill_null(0.0),
            pl.col('source').fill_null(''),
            pl.col('price_usd').fill_null(0.0)
        ])

        # Calculate normalized supply (total_minted - total_burned) / decimals
        # For now, assume decimals = 9 for all (can be improved later)
        df_tokens = df_tokens.with_columns([
            ((pl.col('total_minted') - pl.col('total_burned')) / 1e9).alias('supply')
        ])

        # Calculate market cap = price_usd * supply
        df_tokens = df_tokens.with_columns([
            (pl.col('price_usd') * pl.col('supply')).alias('market_cap_usd')
        ])

        # Add burned normalized
        df_tokens = df_tokens.with_columns([
            (pl.col('total_burned') / 1e9).alias('burned')
        ])

        # Add blockchain column
        df_tokens = df_tokens.with_columns([
            pl.lit('solana').alias('blockchain')
        ])

        # Add placeholder symbol (metadata fetching would be separate for 100M tokens)
        df_tokens = df_tokens.with_columns([
            pl.lit(None).cast(pl.Utf8).alias('symbol')
        ])

        return df_tokens

    def _print_results(self, df: pl.DataFrame):
        """Print final results table."""
        logger.info('=' * 100)
        logger.info('FINAL RESULTS')
        logger.info('=' * 100)

        # Select and order columns for display
        df_display = df.select([
            'mint',
            'blockchain',
            'symbol',
            'price_usd',
            'market_cap_usd',
            'supply',
            'burned',
            'largest_lp_pool_usd',
            'first_tx_date',
            'source'
        ]).head(100)  # Show first 100 for display

        print(df_display)

        logger.info('=' * 100)
        logger.info(f'Total tokens processed: {len(df):,}')
        logger.info(f'Tokens with price data: {df.filter(pl.col("price_usd") > 0).height:,}')
        logger.info(f'Tokens with liquidity data: {df.filter(pl.col("largest_lp_pool_usd") > 0).height:,}')
        logger.info('=' * 100)

    def _print_performance_report(self):
        """Print comprehensive performance metrics."""
        logger.info('')
        logger.info('=' * 100)
        logger.info('PERFORMANCE REPORT')
        logger.info('=' * 100)
        logger.info(f"{'Step':<40} {'Time (s)':<15} {'% of Total':<15}")
        logger.info('-' * 100)

        total_time = self.performance_metrics['total']

        for step, duration in self.performance_metrics.items():
            if step != 'total':
                percentage = (duration / total_time) * 100
                logger.info(f'{step:<40} {duration:>10.2f}s     {percentage:>10.1f}%')

        logger.info('-' * 100)
        logger.info(f'{"TOTAL TIME":<40} {total_time:>10.2f}s     {100.0:>10.1f}%')
        logger.info('=' * 100)


def main():
    logger.info('=' * 100)
    logger.info('SOLANA TOKEN DATA AGGREGATION WORKER (Polars-Optimized)')
    logger.info('=' * 100)

    worker = TokenAggregationWorker()
    try:
        token_count = worker.process_all_tokens()
        logger.info(f'Successfully processed {token_count:,} tokens')
    except Exception as e:
        logger.error(f'Error in main: {e}', exc_info=True)
        raise


if __name__ == '__main__':
    main()
