#!/usr/bin/env python3
"""
Debug script to trace calculations for a specific token.
Shows: pool selection, price calculation, liquidity calculation.

Usage: python debug_token.py <token_address>
Example: python debug_token.py AhhdRu5YZdjVkKR3wbnUDaymVQL2ucjMQ63sZ3LFHsch
"""

import sys
import requests
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

from src.config import Config, setup_logging
from src.database import get_db_client

setup_logging()


def fetch_token_decimals(token_address: str) -> int:
    """Fetch token decimals from Solana RPC."""
    rpc_url = Config.SOLANA_HTTP_RPC_URL
    if not rpc_url:
        print("WARNING: SOLANA_HTTP_RPC_URL not set, using default 6 decimals")
        return 6

    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [token_address, {"encoding": "jsonParsed"}]
        }
        response = requests.post(rpc_url, json=payload, timeout=10)
        result = response.json()

        if result.get('result') and result['result'].get('value'):
            data = result['result']['value'].get('data')
            if isinstance(data, dict) and data.get('parsed'):
                decimals = data['parsed'].get('info', {}).get('decimals')
                if decimals is not None:
                    return int(decimals)
        return 6  # default
    except Exception as e:
        print(f"WARNING: Failed to fetch decimals: {e}")
        return 6

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

    # Fetch token decimals first
    print("Fetching token decimals from RPC...")
    token_decimals = fetch_token_decimals(token_address)
    print(f"Token decimals: {token_decimals}")
    print()

    pools_query = f"""
    SELECT
        source,
        direction,
        base_coin,
        quote_coin,
        base_coin_amount,
        quote_coin_amount,
        base_pool_balance_after,
        quote_pool_balance_after,
        block_time,
        signature
    FROM solana.swaps
    WHERE (base_coin = '{token_address}' OR quote_coin = '{token_address}')
      AND (
          quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}')
          OR base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}')
      )
    ORDER BY block_time DESC
    LIMIT 20
    """

    results = db_client.execute_query_dict(pools_query)
    print(f"Found {len(results)} recent swaps with SOL/USDC/USDT")
    print()

    for i, row in enumerate(results[:10]):
        # Clean addresses
        base_coin_raw = row['base_coin']
        quote_coin_raw = row['quote_coin']
        base_coin_str = base_coin_raw.decode('utf-8').rstrip('\x00') if isinstance(base_coin_raw, bytes) else str(base_coin_raw).rstrip('\x00')
        quote_coin_str = quote_coin_raw.decode('utf-8').rstrip('\x00') if isinstance(quote_coin_raw, bytes) else str(quote_coin_raw).rstrip('\x00')

        base_amount = row['base_coin_amount']
        quote_amount = row['quote_coin_amount']

        # Determine which is token and which is reference
        if base_coin_str == token_address:
            token_amount = base_amount
            ref_amount = quote_amount
            ref_coin = quote_coin_str
        else:
            token_amount = quote_amount
            ref_amount = base_amount
            ref_coin = base_coin_str

        # Get reference decimals
        if ref_coin == SOL_ADDRESS:
            ref_decimals = 9
            ref_name = "SOL"
            ref_price_usd = sol_price_usd
        else:
            ref_decimals = 6
            ref_name = "USDC/USDT"
            ref_price_usd = 1.0

        # Normalize amounts
        token_normalized = token_amount / (10 ** token_decimals)
        ref_normalized = ref_amount / (10 ** ref_decimals)

        # Calculate trade price
        if token_normalized > 0:
            trade_price = (ref_normalized / token_normalized) * ref_price_usd
        else:
            trade_price = 0

        print(f"Trade {i+1}: {row['source']} ({row['direction']})")
        print(f"  Token Amount: {token_normalized:,.6f} | {ref_name} Amount: {ref_normalized:,.6f}")
        print(f"  TRADE PRICE: ${trade_price:.6f}")
        print(f"  Pool After - Base: {row['base_pool_balance_after']:,.0f} | Quote: {row['quote_pool_balance_after']:,.0f}")
        print(f"  Time: {row['block_time']}")
        print()

    # 2. Aggregated pool stats (как делает наш worker)
    print()
    print("2. AGGREGATED POOL STATS (best pool selection)")
    print("-" * 100)

    # ONLY include direct DEX sources with accurate pool balances
    allowed_sources = [
        'pumpfun_bondingcurve',
        'raydium_swap_v4',
        'raydium_swap_cpmm',
        'raydium_swap_clmm',
        'raydium_swap_stable',
        'raydium_bondingcurve',
        'meteora_swap_dlmm',
        'meteora_swap_pools',
        'meteora_swap_damm',
        'meteora_bondingcurve',
        'orca_swap',
        'phoenix_swap',
        'lifinity_swap_v2',
        'pumpswap_swap',
        'degenfund',
    ]
    allowed_sources_sql = ', '.join([f"'{s}'" for s in allowed_sources])
    allowed_sources_filter = f"AND source IN ({allowed_sources_sql})"

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
          {allowed_sources_filter}

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
          {allowed_sources_filter}
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

    # Fetch token decimals once for price calculations
    print("Fetching token decimals from RPC...")
    token_decimals = fetch_token_decimals(token_address)
    print(f"Token decimals: {token_decimals}")
    print()

    for i, row in enumerate(results[:10]):
        # Clean binary strings
        base_coin_raw = row['base_coin']
        quote_coin_raw = row['quote_coin']
        base_coin_str = base_coin_raw.decode('utf-8').rstrip('\x00') if isinstance(base_coin_raw, bytes) else str(base_coin_raw).rstrip('\x00')
        quote_coin_str = quote_coin_raw.decode('utf-8').rstrip('\x00') if isinstance(quote_coin_raw, bytes) else str(quote_coin_raw).rstrip('\x00')

        base_bal = row['latest_base_bal']
        quote_bal = row['latest_quote_bal']

        # Determine token vs reference
        if base_coin_str == token_address:
            token_balance = base_bal
            ref_balance = quote_bal
            ref_coin = quote_coin_str
        else:
            token_balance = quote_bal
            ref_balance = base_bal
            ref_coin = base_coin_str

        # Determine reference decimals
        if ref_coin == SOL_ADDRESS:
            ref_decimals = 9
            ref_price_usd = sol_price_usd
        else:
            ref_decimals = 6
            ref_price_usd = 1.0

        # Calculate price
        token_normalized = token_balance / (10 ** token_decimals)
        ref_normalized = ref_balance / (10 ** ref_decimals)

        if token_normalized > 0:
            price_usd = (ref_normalized / token_normalized) * ref_price_usd
        else:
            price_usd = 0

        # Calculate liquidity (reference side * 2)
        liquidity_usd = ref_normalized * ref_price_usd * 2

        print(f"Pool {i+1}: {row['source']}")
        print(f"  Base: {base_coin_str[:16]}... | Quote: {quote_coin_str[:16]}...")
        print(f"  Token Balance: {token_normalized:,.2f} | Ref Balance: {ref_normalized:,.2f}")
        print(f"  Price USD: ${price_usd:.6f} | Liquidity USD: ${liquidity_usd:,.2f}")
        print()

    # 3. VWAP Price calculation (Trade-Based)
    print()
    print("3. VWAP PRICE CALCULATION (Trade-Based)")
    print("-" * 100)

    vwap_query = f"""
    WITH unified_trades AS (
        SELECT
            base_coin AS token,
            base_coin_amount AS token_amount,
            quote_coin_amount AS ref_amount,
            CASE
                WHEN quote_coin = '{SOL_ADDRESS}' THEN 'SOL'
                WHEN quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
                ELSE 'OTHER'
            END as ref_type,
            block_time
        FROM solana.swaps
        WHERE base_coin = '{token_address}'
          AND (quote_coin = '{SOL_ADDRESS}' OR quote_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}'))
          {allowed_sources_filter}
          AND base_coin_amount > 0 AND quote_coin_amount > 0

        UNION ALL

        SELECT
            quote_coin AS token,
            quote_coin_amount AS token_amount,
            base_coin_amount AS ref_amount,
            CASE
                WHEN base_coin = '{SOL_ADDRESS}' THEN 'SOL'
                WHEN base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}') THEN 'STABLE'
                ELSE 'OTHER'
            END as ref_type,
            block_time
        FROM solana.swaps
        WHERE quote_coin = '{token_address}'
          AND (base_coin = '{SOL_ADDRESS}' OR base_coin IN ('{USDC_ADDRESS}', '{USDT_ADDRESS}'))
          {allowed_sources_filter}
          AND base_coin_amount > 0 AND quote_coin_amount > 0
    )
    SELECT
        -- VWAP 5 minutes
        sumIf(ref_amount, block_time >= now() - INTERVAL 5 MINUTE)
            / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 5 MINUTE), 1) AS vwap_5m_raw,
        countIf(block_time >= now() - INTERVAL 5 MINUTE) AS trades_5m,

        -- VWAP 1 hour
        sumIf(ref_amount, block_time >= now() - INTERVAL 1 HOUR)
            / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 1 HOUR), 1) AS vwap_1h_raw,
        countIf(block_time >= now() - INTERVAL 1 HOUR) AS trades_1h,

        -- VWAP 24 hours
        sumIf(ref_amount, block_time >= now() - INTERVAL 24 HOUR)
            / greatest(sumIf(token_amount, block_time >= now() - INTERVAL 24 HOUR), 1) AS vwap_24h_raw,
        countIf(block_time >= now() - INTERVAL 24 HOUR) AS trades_24h,

        -- Last trade price
        argMax(ref_amount / token_amount, block_time) AS last_price_raw,

        -- Reference type from latest
        argMax(ref_type, block_time) AS latest_ref_type

    FROM unified_trades
    WHERE ref_type != 'OTHER'
    """

    vwap_results = db_client.execute_query_dict(vwap_query)
    if vwap_results:
        vwap = vwap_results[0]
        ref_type = vwap['latest_ref_type']
        ref_decimals = 9 if ref_type == 'SOL' else 6
        ref_price_usd = sol_price_usd if ref_type == 'SOL' else 1.0

        print(f"Reference Type: {ref_type}")
        print(f"Token Decimals: {token_decimals} | Ref Decimals: {ref_decimals}")
        print()

        # Show all VWAP methods
        print("VWAP Methods:")
        print(f"  5 min:  {vwap['trades_5m']:>4} trades | Raw: {vwap['vwap_5m_raw']:.10f}")
        print(f"  1 hour: {vwap['trades_1h']:>4} trades | Raw: {vwap['vwap_1h_raw']:.10f}")
        print(f"  24 hour:{vwap['trades_24h']:>4} trades | Raw: {vwap['vwap_24h_raw']:.10f}")
        print(f"  Last:   {'   1'} trade  | Raw: {vwap['last_price_raw']:.10f}")
        print()

        # Determine which method to use (cascading)
        if vwap['trades_5m'] >= 3:
            price_raw = vwap['vwap_5m_raw']
            method = "VWAP_5M"
        elif vwap['trades_1h'] >= 5:
            price_raw = vwap['vwap_1h_raw']
            method = "VWAP_1H"
        elif vwap['trades_24h'] >= 5:
            price_raw = vwap['vwap_24h_raw']
            method = "VWAP_24H"
        else:
            price_raw = vwap['last_price_raw']
            method = "LAST_ANY"

        # Convert to USD
        price_per_ref = price_raw * (10 ** (token_decimals - ref_decimals))
        price_usd = price_per_ref * ref_price_usd

        print(f"Selected Method: {method}")
        print(f"Price Raw: {price_raw:.10f}")
        print(f"Price per {ref_type}: {price_per_ref:.10f}")
        print(f"PRICE USD: ${price_usd:.6f}")
        print()

    # Keep old pool-based calc for comparison
    if results:
        best_pool = results[0]
        print()
        print("4. POOL-BASED PRICE (for comparison - DEPRECATED)")
        print("-" * 100)

        base_coin = best_pool['base_coin']
        quote_coin = best_pool['quote_coin']
        base_bal = best_pool['latest_base_bal']
        quote_bal = best_pool['latest_quote_bal']

        print(f"Best Pool Source: {best_pool['source']}")
        print(f"Liquidity Score: ${best_pool['liquidity_score_usd']:,.2f}")
        print()

        # Determine which is the token and which is reference
        # Handle binary strings from ClickHouse
        base_coin_str = base_coin.decode('utf-8').rstrip('\x00') if isinstance(base_coin, bytes) else str(base_coin).rstrip('\x00')
        quote_coin_str = quote_coin.decode('utf-8').rstrip('\x00') if isinstance(quote_coin, bytes) else str(quote_coin).rstrip('\x00')

        print(f"Base Coin (cleaned): {base_coin_str}")
        print(f"Quote Coin (cleaned): {quote_coin_str}")
        print(f"Looking for token: {token_address}")
        print()

        if base_coin_str == token_address:
            token_balance = base_bal
            ref_balance = quote_bal
            ref_coin = quote_coin_str
            print("TOKEN is BASE, REFERENCE is QUOTE")
        else:
            token_balance = quote_bal
            ref_balance = base_bal
            ref_coin = base_coin_str
            print("TOKEN is QUOTE, REFERENCE is BASE")

        # Fetch real decimals from RPC
        print()
        print("Fetching token decimals from RPC...")
        token_decimals = fetch_token_decimals(token_address)
        print(f"Token decimals from RPC: {token_decimals}")

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

    # 5. Check mints/burns for supply
    print()
    print("5. SUPPLY DATA (mints - burns)")
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
