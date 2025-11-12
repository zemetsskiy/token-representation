#!/usr/bin/env python3
"""
Universal Token Worker for Scheduled Processing
Processes tokens from specific materialized views based on schedule
"""

import sys
import os
import logging
import argparse
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

from src.config import Config, setup_logging
from src.database import get_db_client, get_postgres_client
from src.core.main import TokenAggregationWorker

setup_logging()
logger = logging.getLogger(__name__)

# View configurations
VIEW_CONFIGS = {
    'sol_1000_swaps_3_days': {
        'view': 'derived.sol_1000_swaps_3_days',
        'description': '1000+ swaps in 3 days',
        'schedule': 'Daily at 00:00 UTC'
    },
    'sol_500_swaps_7_days': {
        'view': 'derived.sol_500_swaps_7_days',
        'description': '500+ swaps in 7 days',
        'schedule': 'Every 5 minutes'
    },
    'sol_100_swaps_30_days': {
        'view': 'derived.sol_100_swaps_30_days',
        'description': '100+ swaps in 30 days',
        'schedule': 'Daily at 00:00 UTC'
    }
}


class ScheduledTokenWorker(TokenAggregationWorker):
    """
    Extended worker that can process tokens from specific views
    """

    def __init__(self, view_name: str):
        super().__init__()
        self.view_name = view_name
        self.postgres_client = get_postgres_client()

        if view_name not in VIEW_CONFIGS:
            raise ValueError(f"Unknown view: {view_name}. Available views: {list(VIEW_CONFIGS.keys())}")

        self.view_config = VIEW_CONFIGS[view_name]
        logger.info(f"Worker initialized for view: {view_name}")
        logger.info(f"Description: {self.view_config['description']}")
        logger.info(f"Schedule: {self.view_config['schedule']}")

    def discover_tokens_from_view(self) -> list:
        """
        Fetch distinct tokens from the specified materialized view.
        Excludes SOL, USDC, USDT (we only want tokens that trade AGAINST these pairs).

        Returns:
            List of unique token addresses
        """
        # Build exclusion list: SOL + all stablecoins
        exclude_tokens = [Config.SOL_ADDRESS] + list(Config.STABLECOINS.values())

        # Create SQL list for exclusion
        exclude_list = ', '.join([f"'{token}'" for token in exclude_tokens])

        query = f"""
        SELECT DISTINCT token
        FROM {self.view_config['view']}
        WHERE token NOT IN ({exclude_list})
        """

        logger.info(f"Fetching DISTINCT tokens from {self.view_config['view']}")
        logger.info(f"Excluding base tokens: SOL, USDC, USDT")
        try:
            result = self.db_client.execute_query_dict(query)

            # Decode binary token addresses to strings and strip null bytes
            tokens = []
            for row in result:
                token_value = row['token']
                if isinstance(token_value, bytes):
                    # Decode bytes and strip null bytes
                    token_str = token_value.decode('utf-8').rstrip('\x00')
                else:
                    token_str = str(token_value).rstrip('\x00')
                tokens.append(token_str)

            logger.info(f"Fetched {len(tokens):,} distinct tokens (after filtering)")
            return tokens

        except Exception as e:
            logger.error(f"Failed to fetch tokens from view: {e}", exc_info=True)
            raise

    def process_view_tokens(self) -> int:
        """
        Process all tokens from the specified view

        Returns:
            Number of tokens processed
        """
        import time
        import polars as pl

        total_start = time.time()
        logger.info('=' * 100)
        logger.info(f'STARTING SCHEDULED TOKEN AGGREGATION')
        logger.info(f'View: {self.view_name}')
        logger.info(f'Description: {self.view_config["description"]}')
        logger.info('=' * 100)

        try:
            # Step 1: Fetch tokens from view
            logger.info('Step 1/2: Fetching tokens from materialized view')
            step_start = time.time()
            all_mints = self.discover_tokens_from_view()
            self.performance_metrics['discover_mints'] = time.time() - step_start

            if len(all_mints) == 0:
                logger.warning('No tokens found in view')
                return 0

            # Step 2: Process in chunks (reuse parent class method)
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

            # Save results to PostgreSQL
            logger.info('')
            logger.info('=' * 100)
            logger.info('SAVING RESULTS TO POSTGRESQL')
            logger.info('=' * 100)
            save_start = time.time()

            # Remove duplicates - keep the last occurrence of each token
            original_count = len(final_df)
            final_df = final_df.unique(subset=['mint'], keep='last')
            deduplicated_count = len(final_df)

            if original_count > deduplicated_count:
                logger.warning(f'Removed {original_count - deduplicated_count:,} duplicate tokens')
                logger.info(f'Final unique tokens: {deduplicated_count:,}')

            try:
                rows_inserted = self.postgres_client.insert_token_metrics_batch(
                    df=final_df,
                    view_source=self.view_name,
                    batch_size=1000
                )
                save_duration = time.time() - save_start
                logger.info(f'Saved {rows_inserted:,} rows to PostgreSQL in {save_duration:.2f}s')

                # Refresh materialized views
                self.postgres_client.refresh_materialized_views()

                # Get statistics
                total_count = self.postgres_client.get_metrics_count()
                logger.info(f'Total metrics in database: {total_count:,}')

            except Exception as e:
                logger.error(f'Failed to save to PostgreSQL: {e}', exc_info=True)
                logger.warning('Results printed but not saved to database')

            logger.info('=' * 100)

            return len(final_df)

        except Exception as e:
            logger.error(f'Error processing tokens: {e}', exc_info=True)
            raise


def main():
    parser = argparse.ArgumentParser(
        description='Universal Token Worker for Scheduled Processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available Views:
  sol_1000_swaps_3_days   - Tokens with 1000+ swaps in last 3 days (run daily at 00:00 UTC)
  sol_500_swaps_7_days    - Tokens with 500+ swaps in last 7 days (run every 5 minutes)
  sol_100_swaps_30_days   - Tokens with 100+ swaps in last 30 days (run daily at 00:00 UTC)

Examples:
  python worker_scheduled.py --view sol_500_swaps_7_days
  python worker_scheduled.py --view sol_1000_swaps_3_days
        """
    )

    parser.add_argument(
        '--view',
        type=str,
        required=True,
        choices=list(VIEW_CONFIGS.keys()),
        help='Materialized view to process'
    )

    parser.add_argument(
        '--list-views',
        action='store_true',
        help='List all available views and exit'
    )

    args = parser.parse_args()

    # List views and exit
    if args.list_views:
        print("\nAvailable Materialized Views:")
        print("=" * 80)
        for view_name, config in VIEW_CONFIGS.items():
            print(f"\nView: {view_name}")
            print(f"  Description: {config['description']}")
            print(f"  Schedule: {config['schedule']}")
            print(f"  Full Path: {config['view']}")
        print("\n")
        return

    # Process tokens
    logger.info('=' * 100)
    logger.info('SOLANA TOKEN WORKER - SCHEDULED PROCESSING')
    logger.info('=' * 100)

    try:
        worker = ScheduledTokenWorker(args.view)
        token_count = worker.process_view_tokens()
        logger.info(f'Successfully processed {token_count:,} tokens from {args.view}')
        sys.exit(0)
    except Exception as e:
        logger.error(f'Worker failed: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
