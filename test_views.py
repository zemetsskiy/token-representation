#!/usr/bin/env python3
"""
Test script for materialized views
Tests each view independently and shows tokens output
"""

import sys
import os
import argparse
from pathlib import Path
from typing import List, Dict

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

from src.config import Config, setup_logging
from src.database import get_db_client

setup_logging()

# View configurations (same as in worker_scheduled.py)
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
        'schedule': 'Daily at 00:10 UTC'
    }
}


def fetch_tokens_from_view(view_name: str) -> List[str]:
    """
    Fetch tokens from specified materialized view

    Args:
        view_name: Name of the view configuration

    Returns:
        List of token addresses
    """
    if view_name not in VIEW_CONFIGS:
        raise ValueError(f"Unknown view: {view_name}")

    view_config = VIEW_CONFIGS[view_name]
    db_client = get_db_client()

    query = f"""
    SELECT token
    FROM {view_config['view']}
    ORDER BY token
    """

    print(f"\n{'=' * 80}")
    print(f"Fetching tokens from: {view_config['view']}")
    print(f"Description: {view_config['description']}")
    print(f"Schedule: {view_config['schedule']}")
    print(f"{'=' * 80}\n")

    try:
        result = db_client.execute_query_dict(query)

        # Decode binary token addresses to strings
        tokens = []
        for row in result:
            token_value = row['token']
            if isinstance(token_value, bytes):
                token_str = token_value.decode('utf-8').rstrip('\x00')
            else:
                token_str = str(token_value).rstrip('\x00')
            tokens.append(token_str)

        return tokens

    except Exception as e:
        print(f"ERROR: Failed to fetch tokens from view: {e}")
        import traceback
        traceback.print_exc()
        return []


def print_token_list(tokens: List[str], view_name: str, limit: int = 20):
    """
    Print formatted token list with statistics

    Args:
        tokens: List of token addresses
        view_name: Name of the view
        limit: Number of tokens to display (default: 20)
    """
    view_config = VIEW_CONFIGS[view_name]

    print(f"\n{'=' * 80}")
    print(f"RESULTS FOR: {view_name}")
    print(f"{'=' * 80}")
    print(f"View: {view_config['view']}")
    print(f"Description: {view_config['description']}")
    print(f"Schedule: {view_config['schedule']}")
    print(f"\nTotal tokens found: {len(tokens):,}")
    print(f"{'=' * 80}\n")

    if len(tokens) == 0:
        print("⚠️  No tokens found in this view!\n")
        return

    # Display first N tokens
    display_limit = min(limit, len(tokens))
    print(f"First {display_limit} tokens:")
    print(f"{'-' * 80}")
    for i, token in enumerate(tokens[:display_limit], 1):
        print(f"{i:3d}. {token}")

    if len(tokens) > display_limit:
        print(f"\n... and {len(tokens) - display_limit:,} more tokens")

    # Display last 5 tokens if there are enough
    if len(tokens) > display_limit + 5:
        print(f"\nLast 5 tokens:")
        print(f"{'-' * 80}")
        for i, token in enumerate(tokens[-5:], len(tokens) - 4):
            print(f"{i:3d}. {token}")

    print(f"\n{'=' * 80}\n")


def test_single_view(view_name: str, limit: int = 20):
    """
    Test a single view and print results

    Args:
        view_name: Name of the view to test
        limit: Number of tokens to display
    """
    print(f"\n🔍 Testing view: {view_name}")
    tokens = fetch_tokens_from_view(view_name)
    print_token_list(tokens, view_name, limit)


def test_all_views(limit: int = 20):
    """
    Test all views and print results

    Args:
        limit: Number of tokens to display per view
    """
    print("\n" + "=" * 80)
    print("TESTING ALL MATERIALIZED VIEWS")
    print("=" * 80)

    results = {}

    for view_name in VIEW_CONFIGS.keys():
        tokens = fetch_tokens_from_view(view_name)
        results[view_name] = tokens
        print_token_list(tokens, view_name, limit)

    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\n{'View':<30} {'Tokens':<15} {'Description':<40}")
    print("-" * 80)

    for view_name, tokens in results.items():
        view_config = VIEW_CONFIGS[view_name]
        print(f"{view_name:<30} {len(tokens):>10,}     {view_config['description']:<40}")

    total_tokens = sum(len(tokens) for tokens in results.values())
    print("-" * 80)
    print(f"{'TOTAL':<30} {total_tokens:>10,}")
    print("=" * 80 + "\n")


def compare_views():
    """
    Compare tokens across all views (overlap analysis)
    """
    print("\n" + "=" * 80)
    print("COMPARING VIEWS - OVERLAP ANALYSIS")
    print("=" * 80 + "\n")

    # Fetch all tokens
    view_tokens = {}
    for view_name in VIEW_CONFIGS.keys():
        print(f"Fetching {view_name}...")
        tokens = fetch_tokens_from_view(view_name)
        view_tokens[view_name] = set(tokens)
        print(f"  → {len(tokens):,} tokens")

    print("\n" + "-" * 80)
    print("OVERLAP ANALYSIS")
    print("-" * 80 + "\n")

    # Check overlaps
    views = list(view_tokens.keys())

    for i, view1 in enumerate(views):
        for view2 in views[i+1:]:
            tokens1 = view_tokens[view1]
            tokens2 = view_tokens[view2]

            overlap = tokens1 & tokens2
            only_in_1 = tokens1 - tokens2
            only_in_2 = tokens2 - tokens1

            print(f"{view1} vs {view2}:")
            print(f"  Common tokens: {len(overlap):,}")
            print(f"  Only in {view1}: {len(only_in_1):,}")
            print(f"  Only in {view2}: {len(only_in_2):,}")

            if len(tokens1) > 0:
                overlap_pct_1 = (len(overlap) / len(tokens1)) * 100
                print(f"  Overlap percentage ({view1}): {overlap_pct_1:.1f}%")

            if len(tokens2) > 0:
                overlap_pct_2 = (len(overlap) / len(tokens2)) * 100
                print(f"  Overlap percentage ({view2}): {overlap_pct_2:.1f}%")

            print()

    # Find tokens in all views
    all_tokens = set.intersection(*view_tokens.values()) if view_tokens else set()
    print("-" * 80)
    print(f"Tokens present in ALL views: {len(all_tokens):,}")

    if all_tokens and len(all_tokens) <= 10:
        print("\nTokens in all views:")
        for token in sorted(all_tokens):
            print(f"  - {token}")

    print("\n" + "=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Test Materialized Views - Check token output',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available Views:
  sol_1000_swaps_3_days   - Tokens with 1000+ swaps in last 3 days
  sol_500_swaps_7_days    - Tokens with 500+ swaps in last 7 days
  sol_100_swaps_30_days   - Tokens with 100+ swaps in last 30 days

Examples:
  # Test single view
  python test_views.py --view sol_500_swaps_7_days

  # Test single view with more tokens displayed
  python test_views.py --view sol_500_swaps_7_days --limit 50

  # Test all views
  python test_views.py --all

  # Compare views (overlap analysis)
  python test_views.py --compare

  # List available views
  python test_views.py --list
        """
    )

    parser.add_argument(
        '--view',
        type=str,
        choices=list(VIEW_CONFIGS.keys()),
        help='Test specific view'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Test all views'
    )

    parser.add_argument(
        '--compare',
        action='store_true',
        help='Compare views and show overlap'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=20,
        help='Number of tokens to display per view (default: 20)'
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available views and exit'
    )

    args = parser.parse_args()

    # List views
    if args.list:
        print("\nAvailable Materialized Views:")
        print("=" * 80)
        for view_name, config in VIEW_CONFIGS.items():
            print(f"\n{view_name}")
            print(f"  View: {config['view']}")
            print(f"  Description: {config['description']}")
            print(f"  Schedule: {config['schedule']}")
        print("\n")
        return

    # Compare views
    if args.compare:
        compare_views()
        return

    # Test all views
    if args.all:
        test_all_views(args.limit)
        return

    # Test single view
    if args.view:
        test_single_view(args.view, args.limit)
        return

    # No arguments - show help
    parser.print_help()


if __name__ == '__main__':
    main()
