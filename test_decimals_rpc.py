#!/usr/bin/env python3
"""
Test script to fetch token decimals via RPC and debug batch requests.
"""

import requests
import json
import sys

RPC_URL = "https://fra.rpc.sharklabs.sh"


def test_single_request(mint_address):
    """Test single RPC request for decimals."""
    print(f"\n{'='*80}")
    print(f"Testing SINGLE request for: {mint_address}")
    print('='*80)

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint_address, {"encoding": "jsonParsed"}]
    }

    print(f"Request payload:")
    print(json.dumps(payload, indent=2))

    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()

        print(f"\nResponse:")
        print(json.dumps(result, indent=2))

        # Try to parse decimals
        try:
            decimals = result['result']['value']['data']['parsed']['info']['decimals']
            print(f"\n✓ SUCCESS: Decimals = {decimals}")
            return decimals
        except Exception as e:
            print(f"\n✗ FAILED to parse decimals: {e}")
            return None

    except Exception as e:
        print(f"\n✗ RPC Error: {e}")
        return None


def test_batch_request(mint_addresses):
    """Test batch RPC request for decimals."""
    print(f"\n{'='*80}")
    print(f"Testing BATCH request for {len(mint_addresses)} mints")
    print('='*80)

    # Build batch payload
    payload = []
    for idx, mint in enumerate(mint_addresses):
        payload.append({
            "jsonrpc": "2.0",
            "id": idx + 1,
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}]
        })

    print(f"Request payload (first item):")
    print(json.dumps(payload[0], indent=2))
    print(f"... and {len(payload) - 1} more requests")

    try:
        response = requests.post(RPC_URL, json=payload, timeout=30)
        response.raise_for_status()
        results = response.json()

        print(f"\nResponse type: {type(results)}")

        # Handle single response wrapped in dict
        if isinstance(results, dict) and 'result' in results:
            print("Got single response dict, wrapping in list")
            results = [results]

        print(f"Number of responses: {len(results) if isinstance(results, list) else 'N/A'}")

        if isinstance(results, list) and len(results) > 0:
            print(f"\nFirst response:")
            print(json.dumps(results[0], indent=2))

            # Try to parse all
            success_count = 0
            for item in results:
                response_id = item.get('id')
                mint_idx = response_id - 1 if response_id else None

                try:
                    decimals = item['result']['value']['data']['parsed']['info']['decimals']
                    mint = mint_addresses[mint_idx] if mint_idx is not None and mint_idx < len(mint_addresses) else "unknown"
                    print(f"  ✓ ID {response_id}: {mint[:8]}... = {decimals} decimals")
                    success_count += 1
                except Exception as e:
                    mint = mint_addresses[mint_idx] if mint_idx is not None and mint_idx < len(mint_addresses) else "unknown"
                    print(f"  ✗ ID {response_id}: {mint[:8]}... - Failed: {e}")

            print(f"\n{'='*80}")
            print(f"Summary: {success_count}/{len(mint_addresses)} successful")
            print('='*80)
        else:
            print(f"\n✗ Unexpected response format:")
            print(json.dumps(results, indent=2))

    except Exception as e:
        print(f"\n✗ RPC Error: {e}")
        import traceback
        traceback.print_exc()


def main():
    # Test mints - some that work, some that may not
    test_mints = [
        "So11111111111111111111111111111111111111112",  # Wrapped SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "44EA892qpA82ionYvaXXEHqM1HizobaC57Du6CFwpump",  # One that fails in logs
        "113NwJVvyTsRM4qHNMS8kS7GEPxT36WbUE88cCNFGAh",  # Another that fails
    ]

    # Allow command line args
    if len(sys.argv) > 1:
        test_mints = sys.argv[1:]

    print("="*80)
    print("Token Decimals RPC Test Script")
    print("="*80)
    print(f"RPC URL: {RPC_URL}")
    print(f"Testing {len(test_mints)} mints")
    print()

    # Test each mint individually first
    print("\n" + "="*80)
    print("PHASE 1: Individual Requests")
    print("="*80)

    for mint in test_mints:
        test_single_request(mint)

    # Test batch request
    print("\n" + "="*80)
    print("PHASE 2: Batch Request")
    print("="*80)

    test_batch_request(test_mints)


if __name__ == "__main__":
    main()
