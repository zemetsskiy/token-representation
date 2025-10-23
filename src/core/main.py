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
    Chunk-optimized token aggregation worker using Polars.
    Processes millions of tokens in manageable chunks to prevent OOM errors.
    """

    def __init__(self):
        logger.info('Initializing Token Aggregation Worker (Chunk-Optimized)')
        self.db_client = get_db_client()
        self.token_discovery = TokenDiscovery(self.db_client)
        self.supply_calculator = SupplyCalculator(self.db_client)
        self.price_calculator = PriceCalculator(self.db_client)
        self.liquidity_analyzer = LiquidityAnalyzer(self.db_client)
        self.first_tx_finder = FirstTxFinder(self.db_client)
        self.performance_metrics = {}
        self.chunk_size = Config.CHUNK_SIZE
        logger.info(f'Chunk size: {self.chunk_size:,} tokens')
        logger.info('Worker initialized successfully')

    def process_all_tokens(self) -> int:
        """
        Process ALL tokens using chunk-based queries and Polars for in-memory processing.
        NO OOM errors - processes data in manageable chunks.
        """
        total_start = time.time()
        logger.info('=' * 100)
        logger.info('STARTING CHUNK-BASED TOKEN AGGREGATION')
        logger.info('=' * 100)

        try:
            # Step 1: Discover all token mints
            logger.info('Step 1/2: Discovering all token mints')
            step_start = time.time()
            mints_data = self.token_discovery.discover_all_token_mints()

            # Extract mint addresses as list
            all_mints = [d['mint'] for d in mints_data]
            self.performance_metrics['discover_mints'] = time.time() - step_start
            logger.info(f'Discovered {len(all_mints):,} total mints in {self.performance_metrics["discover_mints"]:.2f}s')

            if len(all_mints) == 0:
                logger.warning('No tokens found')
                return 0

            # Step 2: Process in chunks
            logger.info(f'Step 2/2: Processing {len(all_mints):,} tokens in chunks of {self.chunk_size:,}')
            step_start = time.time()

            all_results_dfs = []
            total_chunks = (len(all_mints) + self.chunk_size - 1) // self.chunk_size

            for chunk_idx in range(0, len(all_mints), self.chunk_size):
                chunk_mints = all_mints[chunk_idx:chunk_idx + self.chunk_size]
                chunk_num = (chunk_idx // self.chunk_size) + 1

                logger.info('')
                logger.info('=' * 100)
                logger.info(f'CHUNK {chunk_num}/{total_chunks}: Processing {len(chunk_mints):,} tokens')
                logger.info('=' * 100)

                chunk_df = self._process_chunk(chunk_mints, chunk_num)
                all_results_dfs.append(chunk_df)

                logger.info(f'Chunk {chunk_num}/{total_chunks} complete. Rows: {len(chunk_df):,}')

            # Concatenate all chunk results
            logger.info('')
            logger.info('=' * 100)
            logger.info('Concatenating all chunk results...')
            final_df = pl.concat(all_results_dfs)
            self.performance_metrics['process_chunks'] = time.time() - step_start
            logger.info(f'All chunks processed and concatenated in {self.performance_metrics["process_chunks"]:.2f}s')

            # Calculate total time
            self.performance_metrics['total'] = time.time() - total_start

            # Print results
            self._print_results(final_df)
            self._print_performance_report()

            return len(final_df)

        except Exception as e:
            logger.error(f'Error processing tokens: {e}', exc_info=True)
            raise

    def _process_chunk(self, chunk_mints: list, chunk_num: int) -> pl.DataFrame:
        """
        Process a single chunk of tokens using temporary table approach.
        Makes 6 database queries per chunk (supply: 2, first_tx: 2, pools: 1, prices: 1).
        """
        # Step 1: Upload chunk to temporary table
        logger.info(f'  [{chunk_num}] Uploading {len(chunk_mints):,} tokens to temporary table...')
        temp_table_name = 'chunk_tokens'
        chunk_data_for_upload = [[mint] for mint in chunk_mints]  # List of lists
        self.db_client.manage_chunk_table(temp_table_name, chunk_data_for_upload, column_names=['mint'])

        # Create base DataFrame
        df_chunk = pl.DataFrame({'mint': chunk_mints})

        # Step 2: Fetch supply data (2 queries) - processors now use temp table
        logger.info(f'  [{chunk_num}] Fetching supply data...')
        df_supply = self.supply_calculator.get_supplies_for_chunk()
        df_chunk = df_chunk.join(df_supply, on='mint', how='left')

        # Step 3: Fetch first tx dates (2 queries)
        logger.info(f'  [{chunk_num}] Fetching first tx dates...')
        df_first_tx = self.first_tx_finder.get_first_tx_for_chunk()
        df_chunk = df_chunk.join(df_first_tx, on='mint', how='left')

        # Step 4: Fetch prices (1 query)
        logger.info(f'  [{chunk_num}] Fetching prices...')
        df_prices = self.price_calculator.get_prices_for_chunk()
        df_chunk = df_chunk.join(df_prices, on='mint', how='left')

        # Step 5: Fetch pool metrics (1 query)
        logger.info(f'  [{chunk_num}] Fetching pool metrics...')
        pool_data = self.liquidity_analyzer.get_pool_metrics_for_chunk()
        df_chunk = self._process_pools_and_metrics(df_chunk, pool_data, chunk_num)

        return df_chunk

    def _process_pools_and_metrics(self, df_tokens: pl.DataFrame, pool_data: list, chunk_num: int) -> pl.DataFrame:
        """
        Process pool data and calculate liquidity + market cap metrics using Polars.
        All calculations happen in memory - NO database queries.
        """
        logger.info(f'  [{chunk_num}] Processing pools and calculating metrics in memory...')

        if not pool_data or len(pool_data) == 0:
            # No pool data - add empty columns
            df_tokens = df_tokens.with_columns([
                pl.lit(0.0).alias('largest_lp_pool_usd'),
                pl.lit('').alias('source'),
                pl.lit(0.0).alias('supply'),
                pl.lit(0.0).alias('burned'),
                pl.lit(0.0).alias('market_cap_usd'),
                pl.lit('solana').alias('blockchain'),
                pl.lit(None).cast(pl.Utf8).alias('symbol')
            ])
            return df_tokens

        # Create DataFrame with explicit schema to handle large balance values
        df_pools = pl.DataFrame(
            pool_data,
            schema={
                'canonical_source': pl.Utf8,
                'base_coin': pl.Utf8,
                'quote_coin': pl.Utf8,
                'last_base_balance': pl.Float64,
                'last_quote_balance': pl.Float64
            }
        )

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

        # Create token-pool mapping (base and quote sides)
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

        # Combine both sides
        df_token_pools = pl.concat([df_pools_base, df_pools_quote])

        # Get max liquidity per token
        df_best_pools = (
            df_token_pools
            .group_by('mint')
            .agg([
                pl.col('liquidity_usd').max().alias('largest_lp_pool_usd'),
                pl.col('source').first().alias('source')
            ])
        )

        # Join best pools to tokens
        df_tokens = df_tokens.join(df_best_pools, on='mint', how='left')

        # Fill nulls and calculate final metrics
        df_tokens = df_tokens.with_columns([
            pl.col('largest_lp_pool_usd').fill_null(0.0),
            pl.col('source').fill_null(''),
            pl.col('price_usd').fill_null(0.0),
            pl.col('total_minted').fill_null(0),
            pl.col('total_burned').fill_null(0)
        ])

        # Calculate normalized supply (assume decimals = 9 for now)
        df_tokens = df_tokens.with_columns([
            ((pl.col('total_minted') - pl.col('total_burned')) / 1e9).alias('supply'),
            (pl.col('total_burned') / 1e9).alias('burned')
        ])

        # Calculate market cap = price_usd * supply
        df_tokens = df_tokens.with_columns([
            (pl.col('price_usd') * pl.col('supply')).alias('market_cap_usd')
        ])

        # Add blockchain and symbol columns
        df_tokens = df_tokens.with_columns([
            pl.lit('solana').alias('blockchain'),
            pl.lit(None).cast(pl.Utf8).alias('symbol')
        ])

        return df_tokens

    def _print_results(self, df: pl.DataFrame):
        """Print final results table."""
        logger.info('')
        logger.info('=' * 100)
        logger.info('FINAL RESULTS')
        logger.info('=' * 100)

        # Select columns for display
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
        ]).head(10)

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
    logger.info('SOLANA TOKEN DATA AGGREGATION WORKER (Chunk-Optimized with Polars)')
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
