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

    # 2. VWAP Price calculation (Trade-Based)
    print()
    print("2. VWAP PRICE CALCULATION (Trade-Based)")
    print("-" * 100)

    price_usd = 0  # Will be set by VWAP calculation

    # Step 1: Find best pool by liquidity
    best_pool_query = f"""
    WITH unified_swaps AS (
        SELECT
            source,
            base_coin,
            quote_coin,
            quote_pool_balance_after AS ref_balance_raw,
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

        UNION ALL

        SELECT
            source,
            base_coin,
            quote_coin,
            base_pool_balance_after AS ref_balance_raw,
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
    )
    SELECT
        argMax(source, CASE
            WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {sol_price_usd}
            WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
            ELSE 0
        END) AS best_source,
        argMax(base_coin, CASE
            WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {sol_price_usd}
            WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
            ELSE 0
        END) AS best_base_coin,
        argMax(quote_coin, CASE
            WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {sol_price_usd}
            WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
            ELSE 0
        END) AS best_quote_coin,
        argMax(ref_type, CASE
            WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {sol_price_usd}
            WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
            ELSE 0
        END) AS best_ref_type,
        max(CASE
            WHEN ref_type = 'SOL' THEN ref_balance_raw / 1e9 * {sol_price_usd}
            WHEN ref_type = 'STABLE' THEN ref_balance_raw / 1e6
            ELSE 0
        END) AS best_liquidity_usd
    FROM unified_swaps
    WHERE ref_type != 'OTHER'
    """

    best_pool_result = db_client.execute_query_dict(best_pool_query)
    if not best_pool_result or not best_pool_result[0]['best_source']:
        print("No pools found for this token!")
        print()
    else:
        bp = best_pool_result[0]
        best_source = bp['best_source']
        best_base = bp['best_base_coin'].decode('utf-8').rstrip('\x00') if isinstance(bp['best_base_coin'], bytes) else str(bp['best_base_coin']).rstrip('\x00')
        best_quote = bp['best_quote_coin'].decode('utf-8').rstrip('\x00') if isinstance(bp['best_quote_coin'], bytes) else str(bp['best_quote_coin']).rstrip('\x00')
        best_ref_type = bp['best_ref_type']
        best_liq = bp['best_liquidity_usd']

        print(f"Best Pool: {best_source}")
        print(f"  Pair: {best_base[:16]}... / {best_quote[:16]}...")
        print(f"  Ref Type: {best_ref_type}")
        print(f"  Liquidity: ${best_liq:,.2f}")
        print()

        # Step 2: Calculate VWAP only from this specific pool
        vwap_query = f"""
        SELECT
            sumIf(token_amount, block_time >= now() - INTERVAL 5 MINUTE) AS sum_token_5m,
            sumIf(ref_amount, block_time >= now() - INTERVAL 5 MINUTE) AS sum_ref_5m,
            countIf(block_time >= now() - INTERVAL 5 MINUTE) AS trades_5m,

            sumIf(token_amount, block_time >= now() - INTERVAL 1 HOUR) AS sum_token_1h,
            sumIf(ref_amount, block_time >= now() - INTERVAL 1 HOUR) AS sum_ref_1h,
            countIf(block_time >= now() - INTERVAL 1 HOUR) AS trades_1h,

            sumIf(token_amount, block_time >= now() - INTERVAL 24 HOUR) AS sum_token_24h,
            sumIf(ref_amount, block_time >= now() - INTERVAL 24 HOUR) AS sum_ref_24h,
            countIf(block_time >= now() - INTERVAL 24 HOUR) AS trades_24h,

            argMax(ref_amount / token_amount, block_time) AS last_price_raw
        FROM (
            SELECT
                base_coin_amount AS token_amount,
                quote_coin_amount AS ref_amount,
                block_time
            FROM solana.swaps
            WHERE base_coin = '{token_address}'
              AND source = '{best_source}'
              AND base_coin = '{best_base}'
              AND quote_coin = '{best_quote}'
              AND base_coin_amount > 0 AND quote_coin_amount > 0

            UNION ALL

            SELECT
                quote_coin_amount AS token_amount,
                base_coin_amount AS ref_amount,
                block_time
            FROM solana.swaps
            WHERE quote_coin = '{token_address}'
              AND source = '{best_source}'
              AND base_coin = '{best_base}'
              AND quote_coin = '{best_quote}'
              AND base_coin_amount > 0 AND quote_coin_amount > 0
        )
        """

        vwap_results = db_client.execute_query_dict(vwap_query)
        if vwap_results:
            vwap = vwap_results[0]

            # Calculate VWAPs
            vwap_5m = vwap['sum_ref_5m'] / vwap['sum_token_5m'] if vwap['sum_token_5m'] > 0 else 0
            vwap_1h = vwap['sum_ref_1h'] / vwap['sum_token_1h'] if vwap['sum_token_1h'] > 0 else 0
            vwap_24h = vwap['sum_ref_24h'] / vwap['sum_token_24h'] if vwap['sum_token_24h'] > 0 else 0

            print(f"VWAP from Best Pool ({best_source}):")
            print(f"  5 min:  {vwap['trades_5m']:>4} trades | Raw: {vwap_5m:.10f}")
            print(f"  1 hour: {vwap['trades_1h']:>4} trades | Raw: {vwap_1h:.10f}")
            print(f"  24 hour:{vwap['trades_24h']:>4} trades | Raw: {vwap_24h:.10f}")
            print(f"  Last:                  | Raw: {vwap['last_price_raw']:.10f}" if vwap['last_price_raw'] else "  Last:                  | Raw: 0")
            print()

            # Cascading selection
            price_raw = 0
            method = None

            if vwap['trades_5m'] >= 3 and vwap_5m > 0:
                price_raw, method = vwap_5m, f'{best_ref_type}_VWAP_5M'
            elif vwap['trades_1h'] >= 5 and vwap_1h > 0:
                price_raw, method = vwap_1h, f'{best_ref_type}_VWAP_1H'
            elif vwap['trades_24h'] >= 5 and vwap_24h > 0:
                price_raw, method = vwap_24h, f'{best_ref_type}_VWAP_24H'
            elif vwap['last_price_raw'] and vwap['last_price_raw'] > 0:
                price_raw, method = vwap['last_price_raw'], f'{best_ref_type}_LAST'

            if price_raw:
                ref_decimals = 9 if best_ref_type == 'SOL' else 6
                ref_price_usd = sol_price_usd if best_ref_type == 'SOL' else 1.0

                # Convert to USD
                price_per_ref = price_raw * (10 ** (token_decimals - ref_decimals))
                price_usd = price_per_ref * ref_price_usd

                print(f"Selected Method: {method}")
                print(f"Price Raw: {price_raw:.10f}")
                print(f"Price per {best_ref_type}: {price_per_ref:.10f}")
                print(f"PRICE USD: ${price_usd:.6f}")
            else:
                print("No valid VWAP data found in best pool!")
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
