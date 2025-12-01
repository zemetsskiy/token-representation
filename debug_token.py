#!/usr/bin/env python3
"""
Debug script to trace calculations for a specific token.
Shows: pool selection, price calculation, liquidity calculation.

Usage: python debug_token.py <token_address>
Example: python debug_token.py AhhdRu5YZdjVkKR3wbnUDaymVQL2ucjMQ63sZ3LFHsch
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

# Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
USDC_ADDRESS = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
USDT_ADDRESS = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'


def debug_token(token_address: str, sol_price_usd: float = 235.0):
    print("=" * 100)
    print(f"DEBUG TOKEN: {token_address}")
    print(f"SOL Price USD: ${sol_price_usd}")
    print("=" * 100)
    print()

    db_client = get_db_client()

    # 1. Find all pools for this token
    print("1. ALL POOLS FOR THIS TOKEN (last 7 days)")
    print("-" * 100)

    pools_query = f"""
    SELECT
        source,
        base_coin,
        quote_coin,
        base_pool_balance_after,
        quote_pool_balance_after,
        block_time,
        -- Identify reference asset
        CASE
            WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
            WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
            WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
            WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
            ELSE 'OTHER'
        END as ref_type,
        -- Reference balance
        CASE
            WHEN quote_coin = '{SOL_ADDRESS}' THEN quote_pool_balance_after
            WHEN base_coin = '{SOL_ADDRESS}' THEN base_pool_balance_after
            WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN quote_pool_balance_after
            WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN base_pool_balance_after
            ELSE 0
        END as ref_balance_raw
    FROM solana.swaps
    WHERE (base_coin = '{token_address}' OR quote_coin = '{token_address}')
      AND block_time >= now() - INTERVAL 7 DAY
    ORDER BY block_time DESC
    LIMIT 20
    """

    results = db_client.execute_query_dict(pools_query)
    print(f"Found {len(results)} recent swaps")
    print()

    for i, row in enumerate(results[:5]):
        print(f"Swap {i+1}:")
        print(f"  Source: {row['source']}")
        print(f"  Base: {row['base_coin'][:20]}... | Quote: {row['quote_coin'][:20]}...")
        print(f"  Base Balance: {row['base_pool_balance_after']:,.0f}")
        print(f"  Quote Balance: {row['quote_pool_balance_after']:,.0f}")
        print(f"  Ref Type: {row['ref_type']} | Ref Balance Raw: {row['ref_balance_raw']:,.0f}")
        print(f"  Time: {row['block_time']}")
        print()

    # 2. Aggregated pool stats (как делает наш worker)
    print()
    print("2. AGGREGATED POOL STATS (best pool selection)")
    print("-" * 100)

    # Exclude routing/multi-hop sources that don't have real pool balances
    excluded_sources_filter = """
        AND source NOT IN ('orca_swap_twohop', 'jupiter_route', 'raydium_route', 'meteora_route')
        AND source NOT LIKE '%_route%'
        AND source NOT LIKE '%twohop%'
        AND source NOT LIKE '%multihop%'
    """

    agg_query = f"""
    WITH unified_swaps AS (
        SELECT
            base_coin AS token,
            source,
            base_coin,
            quote_coin,
            base_pool_balance_after,
            quote_pool_balance_after,
            block_time,
            CASE
                WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
                WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
                WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
                WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
                ELSE 'OTHER'
            END as ref_type,
            CASE
                WHEN quote_coin = '{SOL_ADDRESS}' THEN quote_pool_balance_after
                WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN quote_pool_balance_after
                WHEN base_coin = '{SOL_ADDRESS}' THEN base_pool_balance_after
                WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN base_pool_balance_after
                ELSE 0
            END as ref_balance_raw
        FROM solana.swaps
        WHERE base_coin = '{token_address}'
          AND (
              quote_coin = '{SOL_ADDRESS}'
              OR quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}')
              OR base_coin = '{SOL_ADDRESS}'
              OR base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}')
          )
          {excluded_sources_filter}

        UNION ALL

        SELECT
            quote_coin AS token,
            source,
            base_coin,
            quote_coin,
            base_pool_balance_after,
            quote_pool_balance_after,
            block_time,
            CASE
                WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
                WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
                WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
                WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
                ELSE 'OTHER'
            END as ref_type,
            CASE
                WHEN quote_coin = '{SOL_ADDRESS}' THEN quote_pool_balance_after
                WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN quote_pool_balance_after
                WHEN base_coin = '{SOL_ADDRESS}' THEN base_pool_balance_after
                WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN base_pool_balance_after
                ELSE 0
            END as ref_balance_raw
        FROM solana.swaps
        WHERE quote_coin = '{token_address}'
          AND (
              quote_coin = '{SOL_ADDRESS}'
              OR quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}')
              OR base_coin = '{SOL_ADDRESS}'
              OR base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}')
          )
          {excluded_sources_filter}
    ),
    pool_stats AS (
        SELECT
            token,
            source,
            base_coin,
            quote_coin,
            argMax(base_pool_balance_after, block_time) as latest_base_bal,
            argMax(quote_pool_balance_after, block_time) as latest_quote_bal,
            argMax(
                CASE
                    WHEN ref_type = 'SOL' THEN (ref_balance_raw / 1e9) * {sol_price_usd}
                    WHEN ref_type = 'STABLE' THEN (ref_balance_raw / 1e6)
                    ELSE 0
                END,
                block_time
            ) as liquidity_score_usd
        FROM unified_swaps
        GROUP BY token, source, base_coin, quote_coin
    )
    SELECT *
    FROM pool_stats
    ORDER BY liquidity_score_usd DESC
    """

    results = db_client.execute_query_dict(agg_query)
    print(f"Found {len(results)} unique pools")
    print()

    for i, row in enumerate(results[:5]):
        print(f"Pool {i+1}:")
        print(f"  Source: {row['source']}")
        print(f"  Base: {row['base_coin'][:20]}...")
        print(f"  Quote: {row['quote_coin'][:20]}...")
        print(f"  Latest Base Balance: {row['latest_base_bal']:,.0f}")
        print(f"  Latest Quote Balance: {row['latest_quote_bal']:,.0f}")
        print(f"  Liquidity Score USD: ${row['liquidity_score_usd']:,.2f}")
        print()

    # 3. Price calculation for best pool
    if results:
        best_pool = results[0]
        print()
        print("3. PRICE CALCULATION (using best pool)")
        print("-" * 100)

        base_coin = best_pool['base_coin']
        quote_coin = best_pool['quote_coin']
        base_bal = best_pool['latest_base_bal']
        quote_bal = best_pool['latest_quote_bal']

        print(f"Best Pool Source: {best_pool['source']}")
        print(f"Liquidity Score: ${best_pool['liquidity_score_usd']:,.2f}")
        print()
        print(f"Base Coin: {base_coin}")
        print(f"Quote Coin: {quote_coin}")
        print(f"Base Balance Raw: {base_bal:,.0f}")
        print(f"Quote Balance Raw: {quote_bal:,.0f}")
        print()

        # Determine which is the token and which is reference
        if base_coin == token_address:
            token_balance = base_bal
            ref_balance = quote_bal
            ref_coin = quote_coin
            token_decimals = 6  # assume, should fetch from RPC
        else:
            token_balance = quote_bal
            ref_balance = base_bal
            ref_coin = base_coin
            token_decimals = 6

        # Determine reference decimals
        if ref_coin == SOL_ADDRESS:
            ref_decimals = 9
            ref_name = "SOL"
            ref_price_usd = sol_price_usd
        else:
            ref_decimals = 6
            ref_name = "STABLE"
            ref_price_usd = 1.0

        print(f"Token Balance Raw: {token_balance:,.0f} (decimals={token_decimals})")
        print(f"Reference ({ref_name}) Balance Raw: {ref_balance:,.0f} (decimals={ref_decimals})")
        print()

        # Normalize
        token_normalized = token_balance / (10 ** token_decimals)
        ref_normalized = ref_balance / (10 ** ref_decimals)

        print(f"Token Normalized: {token_normalized:,.6f}")
        print(f"Reference Normalized: {ref_normalized:,.6f}")
        print()

        # Price calculation
        if token_normalized > 0:
            price_per_ref = ref_normalized / token_normalized
            price_usd = price_per_ref * ref_price_usd

            print(f"Price per {ref_name}: {price_per_ref:.10f}")
            print(f"Price USD: ${price_usd:.10f}")
            print()

            # Liquidity calculation
            liquidity_usd = ref_normalized * ref_price_usd * 2
            print(f"Largest LP Pool USD: ${liquidity_usd:,.2f}")

    # 4. Check mints/burns for supply
    print()
    print("4. SUPPLY DATA (mints - burns)")
    print("-" * 100)

    supply_query = f"""
    SELECT
        (SELECT COALESCE(sum(amount), 0) FROM solana.mints WHERE mint = '{token_address}') as total_minted,
        (SELECT COALESCE(sum(amount), 0) FROM solana.burns WHERE mint = '{token_address}') as total_burned
    """

    supply_result = db_client.execute_query_dict(supply_query)
    if supply_result:
        minted = supply_result[0]['total_minted']
        burned = supply_result[0]['total_burned']
        supply_raw = minted - burned
        supply_normalized = supply_raw / (10 ** 6)  # assume 6 decimals

        print(f"Total Minted Raw: {minted:,.0f}")
        print(f"Total Burned Raw: {burned:,.0f}")
        print(f"Supply Raw: {supply_raw:,.0f}")

        if supply_raw < 0:
            print()
            print("⚠️  WARNING: Burns > Mints! This indicates:")
            print("   - Bridged/wrapped token with incomplete mint history")
            print("   - Or data collection started after token creation")
            print("   - Supply will be set to 0 in production")
            supply_normalized = 0

        print(f"Supply Normalized (6 decimals): {supply_normalized:,.2f}")

        if results and token_normalized > 0 and supply_normalized > 0:
            market_cap = price_usd * supply_normalized
            print()
            print(f"Market Cap USD: ${market_cap:,.2f}")
        elif supply_normalized <= 0:
            print()
            print("⚠️  Market Cap: Cannot calculate (supply <= 0)")

    print()
    print("=" * 100)
    print("DEBUG COMPLETE")
    print("=" * 100)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python debug_token.py <token_address> [sol_price_usd]")
        print("Example: python debug_token.py AhhdRu5YZdjVkKR3wbnUDaymVQL2ucjMQ63sZ3LFHsch 235")
        sys.exit(1)

    token = sys.argv[1]
    sol_price = float(sys.argv[2]) if len(sys.argv) > 2 else 235.0

    debug_token(token, sol_price)
