import logging
import math
import time
from typing import Dict
import polars as pl
from ...config import Config, setup_logging
from ...database import get_db_client, ClickHouseClient
from ...database.postgres import get_postgres_client
from ..processors import (
    TokenDiscovery,
    SupplyCalculator,
    PriceCalculator,
    LiquidityAnalyzer,
    FirstTxFinder,
    DecimalsResolver,
    MetadataFetcher
)

setup_logging()
logger = logging.getLogger(__name__)

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
# SOL price is fetched from Redis at runtime - no hardcoded fallback
SOL_DECIMALS = 9
STABLECOINS = {
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
}
STABLECOIN_DECIMALS = 6
LST_ADDRESSES = {
    'JitoSOL': 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',
    'jupSOL': 'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',
    'mSOL': 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
    'bSOL': 'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',
}
LST_DECIMALS = 9
LOG_10 = math.log(10.0)


class TokenAggregationWorker:
    """
    Chunk-optimized token aggregation worker using Polars.
    Processes millions of tokens in manageable chunks to prevent OOM errors.
    """

    def __init__(self):
        logger.info('Initializing Token Aggregation Worker (Chunk-Optimized)')
        self.db_client = get_db_client()
        
        # Initialize Redis Client (REQUIRED for live SOL price)
        from ...database.redis_client import RedisClient
        self.redis_client = RedisClient()

        # Get SOL Price from Redis (REQUIRED - no fallback)
        self.sol_price_usd = self.redis_client.get_sol_price()
        logger.info(f"Using live SOL price from Redis: ${self.sol_price_usd:.2f}")

        self.token_discovery = TokenDiscovery(self.db_client)
        self.supply_calculator = SupplyCalculator(self.db_client)

        self.price_calculator = PriceCalculator(self.db_client)
        self.price_calculator.set_sol_price(self.sol_price_usd)

        self.liquidity_analyzer = LiquidityAnalyzer(self.db_client)
        self.liquidity_analyzer.set_sol_price(self.sol_price_usd)

        self.first_tx_finder = FirstTxFinder(self.db_client)
        self.decimals_resolver = DecimalsResolver()
        self.metadata_fetcher = MetadataFetcher()
        self.postgres_client = get_postgres_client()
        self.performance_metrics = {}
        self.data_interval_days = 7
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
        """Process a single chunk of tokens using temporary table approach."""
        logger.info(f'  [{chunk_num}] Uploading {len(chunk_mints):,} tokens to temporary table...')
        self.db_client.manage_chunk_table('chunk_tokens', [[mint] for mint in chunk_mints], column_names=['mint'])

        df_chunk = pl.DataFrame({'mint': chunk_mints})

        logger.info(f'  [{chunk_num}] Fetching supply data...')
        df_supply = self.supply_calculator.get_supplies_for_chunk()
        df_chunk = df_chunk.join(df_supply, on='mint', how='left')

        logger.info(f'  [{chunk_num}] Fetching swap data (prices + pools + first_swaps)...')
        swap_data = self.liquidity_analyzer.get_comprehensive_swap_data_for_chunk(interval_days=self.data_interval_days)

        logger.info(f'  [{chunk_num}] Processing first tx dates...')
        df_first_tx = self.first_tx_finder.get_first_tx_for_chunk(first_swaps_data=swap_data['first_swaps'])
        df_chunk = df_chunk.join(df_first_tx, on='mint', how='left')

        logger.info(f'  [{chunk_num}] Checking PostgreSQL for known decimals/metadata...')
        known_data = self.postgres_client.get_known_token_data(chunk_mints)
        if known_data:
            decimals_preloaded = 0
            metadata_preloaded = 0
            supply_preloaded = 0
            for mint, data in known_data.items():
                if data['decimals'] is not None and mint not in self.decimals_resolver.decimals_cache:
                    self.decimals_resolver.decimals_cache[mint] = data['decimals']
                    decimals_preloaded += 1
                if mint not in self.metadata_fetcher.metadata_cache:
                    if data['symbol'] is not None:
                        self.metadata_fetcher.metadata_cache[mint] = (data['symbol'], data['name'], None)
                        metadata_preloaded += 1
                if data.get('supply') and data['supply'] > 0 and data['decimals'] is not None and mint not in self.decimals_resolver.supply_cache:
                    self.decimals_resolver.supply_cache[mint] = int(data['supply'] * (10 ** data['decimals']))
                    supply_preloaded += 1
            logger.info(f'  [{chunk_num}] Pre-loaded from PostgreSQL: {decimals_preloaded} decimals, {metadata_preloaded} metadata, {supply_preloaded} supply')

        logger.info(f'  [{chunk_num}] Fetching decimals via RPC...')
        decimals_map = self.decimals_resolver.resolve_decimals_batch(chunk_mints)
        df_decimals = pl.DataFrame({
            'mint': list(decimals_map.keys()),
            'decimals': list(decimals_map.values())
        })
        df_chunk = df_chunk.join(df_decimals, on='mint', how='left')

        supply_cache = self.decimals_resolver.supply_cache
        if supply_cache:
            rpc_supply_rows = [{'mint': m, 'rpc_supply': float(s)} for m, s in supply_cache.items() if s is not None]
            if rpc_supply_rows:
                df_chunk = df_chunk.join(pl.DataFrame(rpc_supply_rows), on='mint', how='left')
                logger.info(f'  [{chunk_num}] RPC supply data available for {len(rpc_supply_rows):,} tokens')
            else:
                df_chunk = df_chunk.with_columns(pl.lit(None).cast(pl.Int64).alias('rpc_supply'))
        else:
            df_chunk = df_chunk.with_columns(pl.lit(None).cast(pl.Int64).alias('rpc_supply'))

        logger.info(f'  [{chunk_num}] Processing prices...')
        df_prices = self.price_calculator.get_prices_for_chunk(
            price_data=swap_data['prices'],
            decimals_map=decimals_map
        )
        df_chunk = df_chunk.join(df_prices, on='mint', how='left')

        logger.info(f'  [{chunk_num}] Fetching metadata...')
        metadata_map = self.metadata_fetcher.resolve_metadata_batch(chunk_mints)
        metadata_rows = [{'mint': m, 'symbol': s, 'name': n} for m, (s, n, _) in metadata_map.items()]
        if metadata_rows:
            df_chunk = df_chunk.join(pl.DataFrame(metadata_rows), on='mint', how='left')
        else:
            df_chunk = df_chunk.with_columns([
                pl.lit(None).cast(pl.Utf8).alias('symbol'),
                pl.lit(None).cast(pl.Utf8).alias('name')
            ])

        logger.info(f'  [{chunk_num}] Processing pool metrics...')
        df_chunk = self._process_pools_and_metrics(df_chunk, swap_data['pool_data'], chunk_num)

        return df_chunk

    def _process_pools_and_metrics(self, df_tokens: pl.DataFrame, pool_data: list, chunk_num: int) -> pl.DataFrame:
        """Process pool data and calculate liquidity + market cap metrics."""
        logger.info(f'  [{chunk_num}] Processing pools and calculating metrics in memory...')

        if pool_data:
            tokens_with_real_pools = set()
            for pool in pool_data:
                if pool['canonical_source'] != 'pumpfun_bondingcurve':
                    tokens_with_real_pools.add(pool['base_coin'])
                    tokens_with_real_pools.add(pool['quote_coin'])

            if tokens_with_real_pools:
                filtered = []
                removed = 0
                for pool in pool_data:
                    if pool['canonical_source'] == 'pumpfun_bondingcurve':
                        bc, qc = pool['base_coin'], pool['quote_coin']
                        if bc in tokens_with_real_pools or qc in tokens_with_real_pools:
                            removed += 1
                            continue
                    filtered.append(pool)
                if removed:
                    logger.info(f'  [{chunk_num}] Filtered {removed} stale bonding curve pools (tokens migrated to pumpswap/raydium)')
                pool_data = filtered

        if not pool_data or len(pool_data) == 0:
            df_tokens = df_tokens.with_columns([
                pl.lit(0.0).alias('largest_lp_pool_usd'),
                pl.lit(0.0).alias('total_liquidity_usd'),
                pl.lit(None).cast(pl.Datetime).alias('last_trade_time'),
            ])
            df_tokens = df_tokens.with_columns([
                pl.col('price_usd').fill_null(0.0),
                pl.col('total_minted').fill_null(0),
                pl.col('total_burned').fill_null(0),
            ])
            df_tokens = df_tokens.with_columns([
                (pl.col('rpc_supply').cast(pl.Float64) /
                    pl.when(pl.col('decimals').is_not_null())
                    .then(10.0 ** pl.col('decimals'))
                    .otherwise(1e9)
                ).alias('rpc_supply_norm')
            ])
            df_tokens = df_tokens.with_columns([
                pl.max_horizontal(
                    pl.lit(0.0),
                    (pl.col('total_minted').cast(pl.Float64) - pl.col('total_burned').cast(pl.Float64)) /
                    pl.when(pl.col('decimals').is_not_null())
                    .then(10.0 ** pl.col('decimals'))
                    .otherwise(1e9)
                ).alias('ch_supply')
            ])
            df_tokens = df_tokens.with_columns([
                pl.when(pl.col('rpc_supply_norm').is_not_null() & (pl.col('rpc_supply_norm') > 0))
                .then(pl.col('rpc_supply_norm'))
                .when(pl.col('ch_supply') > 0)
                .then(pl.col('ch_supply'))
                .otherwise(0.0)
                .alias('supply')
            ])
            df_tokens = df_tokens.drop(['ch_supply', 'rpc_supply_norm'])
            df_tokens = df_tokens.with_columns([
                (pl.col('price_usd') * pl.col('supply')).alias('market_cap_usd')
            ])
            df_tokens = df_tokens.with_columns([
                pl.lit('solana').alias('chain')
            ])
            return df_tokens

        df_pools = pl.DataFrame(
            pool_data,
            schema={
                'canonical_source': pl.Utf8,
                'base_coin': pl.Utf8,
                'quote_coin': pl.Utf8,
                'last_base_balance': pl.Float64,
                'last_quote_balance': pl.Float64,
                'last_trade_time': pl.Datetime,
            }
        )

        df_token_decimals = (
            df_tokens
            .select(['mint', 'decimals'])
            .rename({'mint': 'token'})
            .with_columns(pl.col('decimals').cast(pl.Float64))
        )
        extra_rows = [
            {'token': SOL_ADDRESS, 'decimals': float(SOL_DECIMALS)},
            {'token': STABLECOINS['USDC'], 'decimals': float(STABLECOIN_DECIMALS)},
            {'token': STABLECOINS['USDT'], 'decimals': float(STABLECOIN_DECIMALS)},
        ]
        for lst_addr in LST_ADDRESSES.values():
            extra_rows.append({'token': lst_addr, 'decimals': float(LST_DECIMALS)})
        extra_decimals = pl.DataFrame(extra_rows)
        df_token_decimals = pl.concat([df_token_decimals, extra_decimals])
        df_token_decimals = df_token_decimals.unique(subset=['token'], keep='last')

        base_decimals = df_token_decimals.rename({'token': 'base_coin', 'decimals': 'base_decimals'})
        quote_decimals = df_token_decimals.rename({'token': 'quote_coin', 'decimals': 'quote_decimals'})

        df_pools = df_pools.join(base_decimals, on='base_coin', how='left')
        df_pools = df_pools.join(quote_decimals, on='quote_coin', how='left')

        df_pools = df_pools.with_columns([
            pl.when(pl.col('base_decimals').is_not_null())
            .then(pl.col('last_base_balance') / ((pl.col('base_decimals') * LOG_10).exp()))
            .otherwise(None)
            .alias('normalized_base_balance'),
            pl.when(pl.col('quote_decimals').is_not_null())
            .then(pl.col('last_quote_balance') / ((pl.col('quote_decimals') * LOG_10).exp()))
            .otherwise(None)
            .alias('normalized_quote_balance')
        ])

        df_pools = df_pools.with_columns([
            pl.col('normalized_base_balance').fill_null(0.0),
            pl.col('normalized_quote_balance').fill_null(0.0)
        ])

        stable_values = list(STABLECOINS.values())
        lst_values = list(LST_ADDRESSES.values())

        df_pools = df_pools.with_columns([
            pl.when(pl.col('base_coin') == SOL_ADDRESS)
            .then(pl.col('normalized_base_balance') * self.sol_price_usd * 2.0)
            .when(pl.col('quote_coin') == SOL_ADDRESS)
            .then(pl.col('normalized_quote_balance') * self.sol_price_usd * 2.0)
            .when(pl.col('base_coin').is_in(lst_values))
            .then(pl.col('normalized_base_balance') * self.sol_price_usd * 2.0)
            .when(pl.col('quote_coin').is_in(lst_values))
            .then(pl.col('normalized_quote_balance') * self.sol_price_usd * 2.0)
            .when(pl.col('base_coin').is_in(stable_values))
            .then(pl.col('normalized_base_balance') * 2.0)
            .when(pl.col('quote_coin').is_in(stable_values))
            .then(pl.col('normalized_quote_balance') * 2.0)
            .otherwise(0.0)
            .alias('liquidity_usd')
        ])

        df_pools_base = df_pools.select([
            pl.col('base_coin').alias('mint'),
            pl.col('liquidity_usd'),
            pl.col('last_trade_time'),
        ])

        df_pools_quote = df_pools.select([
            pl.col('quote_coin').alias('mint'),
            pl.col('liquidity_usd'),
            pl.col('last_trade_time'),
        ])

        df_token_pools = pl.concat([df_pools_base, df_pools_quote])

        df_best_pools = (
            df_token_pools
            .group_by('mint')
            .agg([
                pl.col('liquidity_usd').max().alias('largest_lp_pool_usd'),
                pl.col('liquidity_usd').sum().alias('total_liquidity_usd'),
                pl.col('last_trade_time').max().alias('last_trade_time'),
            ])
        )

        df_tokens = df_tokens.join(df_best_pools, on='mint', how='left')

        df_tokens = df_tokens.with_columns([
            pl.col('largest_lp_pool_usd').fill_null(0.0),
            pl.col('total_liquidity_usd').fill_null(0.0),
            pl.col('price_usd').fill_null(0.0),
            pl.col('total_minted').fill_null(0),
            pl.col('total_burned').fill_null(0),
        ])

        df_tokens = df_tokens.with_columns([
            (pl.col('rpc_supply').cast(pl.Float64) /
                pl.when(pl.col('decimals').is_not_null())
                .then(10.0 ** pl.col('decimals'))
                .otherwise(1e9)
            ).alias('rpc_supply_norm')
        ])

        df_tokens = df_tokens.with_columns([
            pl.max_horizontal(
                pl.lit(0.0),
                (pl.col('total_minted').cast(pl.Float64) - pl.col('total_burned').cast(pl.Float64)) /
                pl.when(pl.col('decimals').is_not_null())
                .then(10.0 ** pl.col('decimals'))
                .otherwise(1e9)
            ).alias('ch_supply')
        ])

        df_tokens = df_tokens.with_columns([
            pl.when(pl.col('rpc_supply_norm').is_not_null() & (pl.col('rpc_supply_norm') > 0))
            .then(pl.col('rpc_supply_norm'))
            .when(pl.col('ch_supply') > 0)
            .then(pl.col('ch_supply'))
            .otherwise(0.0)
            .alias('supply')
        ])
        df_tokens = df_tokens.drop(['ch_supply', 'rpc_supply_norm'])

        df_tokens = df_tokens.with_columns([
            (pl.col('price_usd') * pl.col('supply')).alias('market_cap_usd')
        ])

        df_tokens = df_tokens.with_columns([
            pl.lit('solana').alias('chain')
        ])

        return df_tokens

    def _print_results(self, df: pl.DataFrame):
        """Print final results table."""
        logger.info('')
        logger.info('=' * 100)
        logger.info('FINAL RESULTS (Top 10 by Market Cap)')
        logger.info('=' * 100)

        # Select columns for display, sort by market_cap_usd descending
        df_display = df.select([
            'mint',
            'chain',
            'symbol',
            'decimals',
            'price_usd',
            'market_cap_usd',
            'supply',
            'largest_lp_pool_usd',
            'first_tx_date'
        ]).sort('market_cap_usd', descending=True).head(10)

        print(df_display)

        # Decimals statistics
        total_tokens = len(df)
        tokens_with_decimals = df.filter(pl.col('decimals').is_not_null()).height
        tokens_without_decimals = total_tokens - tokens_with_decimals

        logger.info('=' * 100)
        logger.info(f'Total tokens processed: {total_tokens:,}')
        logger.info(f'Tokens with decimals: {tokens_with_decimals:,} ({100*tokens_with_decimals/total_tokens:.1f}%)')
        logger.info(f'Tokens WITHOUT decimals: {tokens_without_decimals:,} ({100*tokens_without_decimals/total_tokens:.1f}%)')
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
