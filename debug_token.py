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

    # ONLY include direct DEX sources
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

    # 2. VWAP Price calculation (Trade-Based from ALL trades)
    print()
    print("2. VWAP PRICE CALCULATION (ALL Trades - picks ref_type with more activity)")
    print("-" * 100)

    price_usd = 0  # Will be set by VWAP calculation

    # Calculate VWAP separately for SOL and STABLE from ALL trades
    vwap_query = f"""
    WITH unified_swaps AS (
        -- Token is base_coin, reference is quote_coin
        SELECT
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

        -- Token is quote_coin, reference is base_coin
        SELECT
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
        -- SOL VWAP metrics
        sumIf(ref_amount, ref_type = 'SOL' AND block_time >= now() - INTERVAL 5 MINUTE) /
            nullIf(sumIf(token_amount, ref_type = 'SOL' AND block_time >= now() - INTERVAL 5 MINUTE), 0) AS sol_vwap_5m,
        countIf(ref_type = 'SOL' AND block_time >= now() - INTERVAL 5 MINUTE) AS sol_trades_5m,

        sumIf(ref_amount, ref_type = 'SOL' AND block_time >= now() - INTERVAL 1 HOUR) /
            nullIf(sumIf(token_amount, ref_type = 'SOL' AND block_time >= now() - INTERVAL 1 HOUR), 0) AS sol_vwap_1h,
        countIf(ref_type = 'SOL' AND block_time >= now() - INTERVAL 1 HOUR) AS sol_trades_1h,

        sumIf(ref_amount, ref_type = 'SOL' AND block_time >= now() - INTERVAL 24 HOUR) /
            nullIf(sumIf(token_amount, ref_type = 'SOL' AND block_time >= now() - INTERVAL 24 HOUR), 0) AS sol_vwap_24h,
        countIf(ref_type = 'SOL' AND block_time >= now() - INTERVAL 24 HOUR) AS sol_trades_24h,

        argMaxIf(ref_amount / token_amount, block_time, ref_type = 'SOL') AS sol_last,

        -- STABLE VWAP metrics
        sumIf(ref_amount, ref_type = 'STABLE' AND block_time >= now() - INTERVAL 5 MINUTE) /
            nullIf(sumIf(token_amount, ref_type = 'STABLE' AND block_time >= now() - INTERVAL 5 MINUTE), 0) AS stable_vwap_5m,
        countIf(ref_type = 'STABLE' AND block_time >= now() - INTERVAL 5 MINUTE) AS stable_trades_5m,

        sumIf(ref_amount, ref_type = 'STABLE' AND block_time >= now() - INTERVAL 1 HOUR) /
            nullIf(sumIf(token_amount, ref_type = 'STABLE' AND block_time >= now() - INTERVAL 1 HOUR), 0) AS stable_vwap_1h,
        countIf(ref_type = 'STABLE' AND block_time >= now() - INTERVAL 1 HOUR) AS stable_trades_1h,

        sumIf(ref_amount, ref_type = 'STABLE' AND block_time >= now() - INTERVAL 24 HOUR) /
            nullIf(sumIf(token_amount, ref_type = 'STABLE' AND block_time >= now() - INTERVAL 24 HOUR), 0) AS stable_vwap_24h,
        countIf(ref_type = 'STABLE' AND block_time >= now() - INTERVAL 24 HOUR) AS stable_trades_24h,

        argMaxIf(ref_amount / token_amount, block_time, ref_type = 'STABLE') AS stable_last
    FROM unified_swaps
    WHERE ref_type != 'OTHER'
    """

    vwap_results = db_client.execute_query_dict(vwap_query)
    if vwap_results:
        vwap = vwap_results[0]

        # Display SOL VWAP data
        print("SOL Trades VWAP:")
        print(f"  5 min:  {vwap['sol_trades_5m']:>4} trades | Raw: {vwap['sol_vwap_5m'] or 0:.10f}")
        print(f"  1 hour: {vwap['sol_trades_1h']:>4} trades | Raw: {vwap['sol_vwap_1h'] or 0:.10f}")
        print(f"  24 hour:{vwap['sol_trades_24h']:>4} trades | Raw: {vwap['sol_vwap_24h'] or 0:.10f}")
        print(f"  Last:                  | Raw: {vwap['sol_last'] or 0:.10f}")
        print()

        # Display STABLE VWAP data
        print("STABLE Trades VWAP:")
        print(f"  5 min:  {vwap['stable_trades_5m']:>4} trades | Raw: {vwap['stable_vwap_5m'] or 0:.10f}")
        print(f"  1 hour: {vwap['stable_trades_1h']:>4} trades | Raw: {vwap['stable_vwap_1h'] or 0:.10f}")
        print(f"  24 hour:{vwap['stable_trades_24h']:>4} trades | Raw: {vwap['stable_vwap_24h'] or 0:.10f}")
        print(f"  Last:                  | Raw: {vwap['stable_last'] or 0:.10f}")
        print()

        # Pick ref_type with more 24h trades
        sol_trades_24h = vwap['sol_trades_24h'] or 0
        stable_trades_24h = vwap['stable_trades_24h'] or 0

        print(f"Activity comparison: SOL={sol_trades_24h} trades vs STABLE={stable_trades_24h} trades (24h)")

        # Cascading selection based on which has more activity
        price_raw = 0
        method = None
        selected_ref_type = None

        if sol_trades_24h > stable_trades_24h:
            # SOL has more activity
            selected_ref_type = 'SOL'
            if vwap['sol_trades_5m'] >= 3 and vwap['sol_vwap_5m']:
                price_raw, method = vwap['sol_vwap_5m'], 'SOL_VWAP_5M'
            elif vwap['sol_trades_1h'] >= 5 and vwap['sol_vwap_1h']:
                price_raw, method = vwap['sol_vwap_1h'], 'SOL_VWAP_1H'
            elif vwap['sol_trades_24h'] >= 5 and vwap['sol_vwap_24h']:
                price_raw, method = vwap['sol_vwap_24h'], 'SOL_VWAP_24H'
            elif vwap['sol_last'] and vwap['sol_last'] > 0:
                price_raw, method = vwap['sol_last'], 'SOL_LAST'
        else:
            # STABLE has more or equal activity
            selected_ref_type = 'STABLE'
            if vwap['stable_trades_5m'] >= 3 and vwap['stable_vwap_5m']:
                price_raw, method = vwap['stable_vwap_5m'], 'STABLE_VWAP_5M'
            elif vwap['stable_trades_1h'] >= 5 and vwap['stable_vwap_1h']:
                price_raw, method = vwap['stable_vwap_1h'], 'STABLE_VWAP_1H'
            elif vwap['stable_trades_24h'] >= 5 and vwap['stable_vwap_24h']:
                price_raw, method = vwap['stable_vwap_24h'], 'STABLE_VWAP_24H'
            elif vwap['stable_last'] and vwap['stable_last'] > 0:
                price_raw, method = vwap['stable_last'], 'STABLE_LAST'
            # Fallback to SOL if STABLE has no data
            elif vwap['sol_last'] and vwap['sol_last'] > 0:
                selected_ref_type = 'SOL'
                price_raw, method = vwap['sol_last'], 'SOL_LAST'

        if price_raw:
            ref_decimals = 9 if selected_ref_type == 'SOL' else 6
            ref_price_usd = sol_price_usd if selected_ref_type == 'SOL' else 1.0

            # Convert to USD
            price_per_ref = price_raw * (10 ** (token_decimals - ref_decimals))
            price_usd = price_per_ref * ref_price_usd

            print()
            print(f"Selected: {selected_ref_type} (more active)")
            print(f"Method: {method}")
            print(f"Price Raw: {price_raw:.10f}")
            print(f"Price per {selected_ref_type}: {price_per_ref:.10f}")
            print(f"PRICE USD: ${price_usd:.6f}")
        else:
            print()
            print("No valid VWAP data found!")
    print()

    # 3. Check mints/burns for supply
    print()
    print("3. SUPPLY DATA (mints - burns)")
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
        supply_normalized = supply_raw / (10 ** token_decimals)

        print(f"Total Minted Raw: {minted:,.0f}")
        print(f"Total Burned Raw: {burned:,.0f}")
        print(f"Supply Raw: {supply_raw:,.0f}")

        if supply_raw < 0:
            print()
            print("WARNING: Burns > Mints! This indicates:")
            print("   - Bridged/wrapped token with incomplete mint history")
            print("   - Or data collection started after token creation")
            print("   - Supply will be set to 0 in production")
            supply_normalized = 0

        print(f"Supply Normalized ({token_decimals} decimals): {supply_normalized:,.2f}")

        if supply_normalized > 0 and price_usd > 0:
            market_cap = price_usd * supply_normalized
            print()
            print(f"Market Cap USD: ${market_cap:,.2f}")
        elif supply_normalized <= 0:
            print()
            print("WARNING: Market Cap: Cannot calculate (supply <= 0)")

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
