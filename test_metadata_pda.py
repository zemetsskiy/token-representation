#!/usr/bin/env python3
"""
Test script to derive Metaplex metadata PDAs and fetch them via RPC.
"""

import base58
import hashlib
import requests
import json
import sys

METAPLEX_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
RPC_URL = "https://fra.rpc.sharklabs.sh"


def find_program_address(seeds, program_id_bytes):
    """Find a valid program derived address."""
    for bump in range(256, 0, -1):
        try:
            seeds_with_bump = seeds + [bytes([bump - 1])]
            pda = create_program_address(seeds_with_bump, program_id_bytes)
            return pda, bump - 1
        except ValueError:
            continue
    raise ValueError("Unable to find a viable program address bump seed")


def create_program_address(seeds, program_id_bytes):
    """Create a program address (PDA)."""
    hasher = hashlib.sha256()
    for seed in seeds:
        hasher.update(seed)
    hasher.update(program_id_bytes)
    hasher.update(b"ProgramDerivedAddress")
    pda = hasher.digest()
    return pda


def derive_metadata_pda(mint_address):
    """Derive the Metaplex metadata PDA for a mint."""
    try:
        program_id_bytes = base58.b58decode(METAPLEX_PROGRAM_ID)
        mint_bytes = base58.b58decode(mint_address)

        seeds = [
            b"metadata",
            program_id_bytes,
            mint_bytes
        ]

        pda, bump = find_program_address(seeds, program_id_bytes)
        pda_address = base58.b58encode(pda).decode('utf-8')
        return pda_address, bump
    except Exception as e:
        print(f"Error deriving PDA: {e}")
        return None, None


def fetch_metadata(pda_address):
    """Fetch metadata account from RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [pda_address, {"encoding": "base64"}]
    }

    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"RPC Error: {e}")
        return None


def main():
    # Test tokens
    test_mints = [
        "So11111111111111111111111111111111111111112",  # SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "2Q3Zms1UeCRCNXnXo1uZ8Fdf8KftE7fJRDXF884Bpump",  # Random pump token
    ]

    # Allow command line args
    if len(sys.argv) > 1:
        test_mints = sys.argv[1:]

    print("=" * 80)
    print("Metaplex Metadata PDA Tester")
    print("=" * 80)
    print()

    for mint in test_mints:
        print(f"Testing mint: {mint}")
        print("-" * 80)

        # Derive PDA
        pda, bump = derive_metadata_pda(mint)
        if pda:
            print(f"  Metadata PDA: {pda}")
            print(f"  Bump seed: {bump}")

            # Fetch metadata
            print(f"  Fetching metadata from RPC...")
            result = fetch_metadata(pda)

            if result:
                if result.get('result', {}).get('value'):
                    print(f"  ✓ Metadata account exists!")
                    account_data = result['result']['value']
                    print(f"  Owner: {account_data.get('owner')}")
                    print(f"  Lamports: {account_data.get('lamports')}")

                    # Try to parse basic info
                    data = account_data.get('data')
                    if data and isinstance(data, list):
                        print(f"  Data size: {len(data[0]) if data else 0} bytes (base64)")
                else:
                    print(f"  ✗ Metadata account does not exist (no data)")
            else:
                print(f"  ✗ RPC request failed")
        else:
            print(f"  ✗ Failed to derive PDA")

        print()

    print("=" * 80)
    print("\nTo fetch metadata via curl:")
    if pda:
        print(f"""
curl {RPC_URL} \\
  -X POST \\
  -H "Content-Type: application/json" \\
  -d '{{
    "jsonrpc":"2.0",
    "id":1,
    "method":"getAccountInfo",
    "params": ["{pda}", {{"encoding":"base64"}}]
  }}'
""")


if __name__ == "__main__":
    main()
