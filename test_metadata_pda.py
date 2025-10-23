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
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=27f6c1e2-a8d1-4efc-a786-3cbf64c58a3a"


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


def parse_metadata(data_base64):
    """Parse Metaplex metadata from base64 data."""
    try:
        import base64
        import struct

        data_bytes = base64.b64decode(data_base64)
        print(f"    Raw data length: {len(data_bytes)} bytes")

        if len(data_bytes) < 65:
            print(f"    Data too short")
            return

        # Skip key (1), update_authority (32), mint (32)
        offset = 65

        # Read name
        if offset + 4 > len(data_bytes):
            return
        name_len = struct.unpack('<I', data_bytes[offset:offset+4])[0]
        offset += 4
        if offset + name_len > len(data_bytes):
            return
        name = data_bytes[offset:offset+name_len].decode('utf-8', errors='ignore').rstrip('\x00')
        print(f"    Name: {name}")
        offset += 32  # Fixed size in struct

        # Read symbol
        if offset + 4 > len(data_bytes):
            return
        symbol_len = struct.unpack('<I', data_bytes[offset:offset+4])[0]
        offset += 4
        if offset + symbol_len > len(data_bytes):
            return
        symbol = data_bytes[offset:offset+symbol_len].decode('utf-8', errors='ignore').rstrip('\x00')
        print(f"    Symbol: {symbol}")
        offset += 10  # Fixed size in struct

        # Read URI
        if offset + 4 > len(data_bytes):
            return
        uri_len = struct.unpack('<I', data_bytes[offset:offset+4])[0]
        offset += 4
        if offset + uri_len > len(data_bytes):
            return
        uri = data_bytes[offset:offset+uri_len].decode('utf-8', errors='ignore').rstrip('\x00')
        print(f"    URI: {uri}")

    except Exception as e:
        print(f"    Parse error: {e}")


def main():
    # Test tokens - known tokens with metadata
    test_mints = [
        "So11111111111111111111111111111111111111112",  # Wrapped SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    ]

    # Allow command line args
    if len(sys.argv) > 1:
        test_mints = sys.argv[1:]

    print("=" * 80)
    print("Metaplex Metadata PDA Tester")
    print("=" * 80)
    print(f"RPC URL: {RPC_URL}")
    print(f"Metaplex Program: {METAPLEX_PROGRAM_ID}")
    print()

    success_count = 0
    for mint in test_mints:
        print(f"Testing mint: {mint}")
        print("-" * 80)

        # Derive PDA
        pda, bump = derive_metadata_pda(mint)
        if pda:
            print(f"  ✓ Derived PDA: {pda}")
            print(f"    Bump seed: {bump}")

            # Fetch metadata
            print(f"  Fetching from RPC...")
            result = fetch_metadata(pda)

            if result:
                if result.get('result', {}).get('value'):
                    print(f"  ✓ Metadata account EXISTS!")
                    account_data = result['result']['value']
                    print(f"    Owner: {account_data.get('owner')}")
                    print(f"    Lamports: {account_data.get('lamports')}")

                    # Parse metadata
                    data = account_data.get('data')
                    if data and isinstance(data, list) and len(data) > 0:
                        print(f"  Parsing metadata...")
                        parse_metadata(data[0])
                        success_count += 1
                else:
                    print(f"  ✗ Metadata account does NOT exist")
            else:
                print(f"  ✗ RPC request FAILED")
        else:
            print(f"  ✗ Failed to derive PDA")

        print()

    print("=" * 80)
    print(f"Summary: {success_count}/{len(test_mints)} tokens have metadata")
    print("=" * 80)

    # Print example curl command
    if test_mints:
        mint = test_mints[0]
        pda, _ = derive_metadata_pda(mint)
        if pda:
            print(f"\nExample curl command for {mint}:")
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
