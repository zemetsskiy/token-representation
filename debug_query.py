#!/usr/bin/env python3
"""
Debug script to manually test ClickHouse queries.
Usage: python debug_query.py
"""

import sys
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

from src.config import Config, setup_logging
from src.database import get_db_client

setup_logging()

def main():
    print("=" * 80)
    print("ClickHouse Query Debugger")
    print("=" * 80)
    print(f"Host: {Config.CLICKHOUSE_HOST}:{Config.CLICKHOUSE_PORT}")
    print(f"Database: {Config.CLICKHOUSE_DATABASE}")
    print(f"Temp Database: {Config.CLICKHOUSE_TEMP_DATABASE}")
    print("=" * 80)
    print()

    # Connect to ClickHouse
    db_client = get_db_client()
    print("✅ Connected to ClickHouse")
    print()

    # Example: Check if temp table exists
    print("Example 1: Check temp table")
    print("-" * 80)
    query1 = f"""
    SELECT count(*) as row_count
    FROM {Config.CLICKHOUSE_TEMP_DATABASE}.chunk_tokens
    """
    print(f"Query: {query1}")
    try:
        result = db_client.execute_query_dict(query1)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
    print()

    # Example: Check mints table
    print("Example 2: Sample from mints table")
    print("-" * 80)
    query2 = f"""
    SELECT
        mint,
        amount,
        block_time
    FROM {Config.CLICKHOUSE_DATABASE}.mints
    WHERE mint IN (
        SELECT mint
        FROM {Config.CLICKHOUSE_TEMP_DATABASE}.chunk_tokens
        LIMIT 5
    )
    LIMIT 10
    """
    print(f"Query: {query2}")
    try:
        result = db_client.execute_query_dict(query2)
        print(f"Result ({len(result)} rows):")
        for row in result[:3]:
            print(f"  {row}")
    except Exception as e:
        print(f"Error: {e}")
    print()

    # Example: Check swaps table
    print("Example 3: Sample from swaps table")
    print("-" * 80)
    query3 = f"""
    SELECT
        input_mint,
        output_mint,
        block_time,
        canonical_source
    FROM {Config.CLICKHOUSE_DATABASE}.swaps
    WHERE input_mint IN (
        SELECT mint
        FROM {Config.CLICKHOUSE_TEMP_DATABASE}.chunk_tokens
        LIMIT 5
    )
    OR output_mint IN (
        SELECT mint
        FROM {Config.CLICKHOUSE_TEMP_DATABASE}.chunk_tokens
        LIMIT 5
    )
    LIMIT 10
    """
    print(f"Query: {query3}")
    try:
        result = db_client.execute_query_dict(query3)
        print(f"Result ({len(result)} rows):")
        for row in result[:3]:
            print(f"  {row}")
    except Exception as e:
        print(f"Error: {e}")
    print()

    # Interactive mode
    print("=" * 80)
    print("Interactive Mode")
    print("Enter your SQL query (or 'quit' to exit):")
    print("=" * 80)

    while True:
        print()
        query = input("SQL> ").strip()

        if query.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break

        if not query:
            continue

        try:
            result = db_client.execute_query_dict(query)
            print(f"✅ Query completed: {len(result)} rows")
            if result:
                print("First 10 rows:")
                for i, row in enumerate(result[:10], 1):
                    print(f"  {i}. {row}")
            else:
                print("No results returned")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == '__main__':
    main()
