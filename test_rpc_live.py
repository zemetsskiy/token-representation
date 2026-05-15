"""
Live RPC test: verify PDA derivation and supply extraction against real Solana node.
Run on VDS with: python3 test_rpc_live.py
"""
import hashlib
import struct
import base64
import json
import sys
import requests

try:
    import base58
except ImportError:
    print("Installing base58...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "base58", "-q"])
    import base58

RPC_URL = "http://89.42.231.16:8899"
METAPLEX_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

# === ed25519 curve check (same as in metadata_fetcher.py) ===
def is_on_curve(pubkey: bytes) -> bool:
    p = (1 << 255) - 19
    y = int.from_bytes(pubkey, 'little') & ((1 << 255) - 1)
    if y >= p:
        return False
    d = (-121665 * pow(121666, p - 2, p)) % p
    y2 = (y * y) % p
    num = (y2 - 1) % p
    den = (d * y2 + 1) % p
    if den == 0:
        return num == 0
    x2 = (num * pow(den, p - 2, p)) % p
    if x2 == 0:
        return True
    return pow(x2, (p - 1) // 2, p) == 1

def create_program_address(seeds, program_id):
    hasher = hashlib.sha256()
    for seed in seeds:
        hasher.update(seed)
    hasher.update(program_id)
    hasher.update(b"ProgramDerivedAddress")
    pda = hasher.digest()
    if is_on_curve(pda):
        raise ValueError("Address is on curve")
    return pda

def find_program_address(seeds, program_id):
    for bump in range(256, 0, -1):
        try:
            seeds_with_bump = seeds + [bytes([bump - 1])]
            pda = create_program_address(seeds_with_bump, program_id)
            return pda, bump - 1
        except ValueError:
            continue
    raise ValueError("Unable to find a viable program address bump seed")

def derive_metadata_pda(mint_address):
    program_id_bytes = base58.b58decode(METAPLEX_PROGRAM_ID)
    mint_bytes = base58.b58decode(mint_address)
    seeds = [b"metadata", program_id_bytes, mint_bytes]
    pda, bump = find_program_address(seeds, program_id_bytes)
    return base58.b58encode(pda).decode('utf-8'), bump

def read_string(data, offset):
    if offset + 4 > len(data):
        return None
    length = struct.unpack('<I', data[offset:offset + 4])[0]
    if length == 0 or offset + 4 + length > len(data):
        return None
    string_data = data[offset + 4:offset + 4 + length]
    decoded = string_data.decode('utf-8', errors='ignore').rstrip('\x00').strip()
    return decoded if decoded else None

def rpc_call(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = requests.post(RPC_URL, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ============================================================
# TEST 1: PDA derivation — fetch USDC metadata via derived PDA
# ============================================================
def test_pda_and_metadata():
    print("=" * 70)
    print("TEST 1: PDA derivation + metadata fetch")
    print("=" * 70)

    test_tokens = {
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
        "So11111111111111111111111111111111111111112": "SOL (Wrapped)",
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": "mSOL",
        "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn": "JitoSOL",
        "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj": "stSOL",
    }

    results = {}
    for mint, expected_name in test_tokens.items():
        pda, bump = derive_metadata_pda(mint)
        print(f"\n{expected_name} ({mint[:12]}...)")
        print(f"  PDA: {pda} (bump={bump})")

        # Fetch PDA account from RPC
        resp = rpc_call("getAccountInfo", [pda, {"encoding": "base64"}])
        value = resp.get("result", {}).get("value")

        if value is None:
            print(f"  RESULT: Account NOT found at PDA (metadata does not exist)")
            results[expected_name] = False
            continue

        # Parse Metaplex metadata
        data_bytes = base64.b64decode(value["data"][0])
        print(f"  Account data: {len(data_bytes)} bytes")

        offset = 65  # key(1) + update_authority(32) + mint(32)
        name = read_string(data_bytes, offset)
        offset += 4 + 32
        symbol = read_string(data_bytes, offset)
        offset += 4 + 10
        uri = read_string(data_bytes, offset)

        print(f"  RESULT: symbol={symbol}, name={name}, uri={uri[:50] if uri else None}...")
        results[expected_name] = symbol is not None

    print("\n" + "=" * 70)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"PDA + Metadata: {passed}/{total} tokens resolved successfully")
    for name, ok in results.items():
        print(f"  {'PASS' if ok else 'FAIL'}: {name}")
    return all(results.values())

# ============================================================
# TEST 2: Supply extraction from getAccountInfo (jsonParsed)
# ============================================================
def test_supply_extraction():
    print("\n" + "=" * 70)
    print("TEST 2: Supply extraction from RPC (getAccountInfo jsonParsed)")
    print("=" * 70)

    test_tokens = {
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": ("USDC", 6),
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": ("mSOL", 9),
        "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn": ("JitoSOL", 9),
        "So11111111111111111111111111111111111111112": ("SOL", 9),
    }

    all_ok = True
    for mint, (name, expected_decimals) in test_tokens.items():
        resp = rpc_call("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
        value = resp.get("result", {}).get("value")

        if value is None:
            print(f"\n{name}: Account NOT found")
            all_ok = False
            continue

        try:
            info = value["data"]["parsed"]["info"]
            decimals = info["decimals"]
            supply_str = info.get("supply")
            supply = int(supply_str) if supply_str is not None else None
            supply_human = supply / (10 ** decimals) if supply is not None else None

            print(f"\n{name} ({mint[:12]}...):")
            print(f"  decimals={decimals} (expected={expected_decimals}) {'OK' if decimals == expected_decimals else 'MISMATCH!'}")
            print(f"  supply_raw={supply}")
            print(f"  supply_human={supply_human:,.2f}" if supply_human else "  supply_human=None")

            if decimals != expected_decimals:
                all_ok = False
            if supply is None or supply == 0:
                print(f"  WARNING: supply is {supply}")
        except Exception as e:
            print(f"\n{name}: PARSE ERROR: {e}")
            all_ok = False

    print("\n" + "=" * 70)
    print(f"Supply extraction: {'ALL PASS' if all_ok else 'SOME FAILURES'}")
    return all_ok

# ============================================================
# TEST 3: Batch RPC (same as production code)
# ============================================================
def test_batch_rpc():
    print("\n" + "=" * 70)
    print("TEST 3: Batch RPC request (like production)")
    print("=" * 70)

    mints = [
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    ]

    payload = []
    for idx, mint in enumerate(mints):
        payload.append({
            "jsonrpc": "2.0",
            "id": idx + 1,
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}]
        })

    resp = requests.post(RPC_URL, json=payload, timeout=30)
    resp.raise_for_status()
    results = resp.json()

    if isinstance(results, dict):
        results = [results]

    print(f"Sent batch of {len(mints)} requests, got {len(results)} responses")

    for item in results:
        rid = item.get("id")
        mint = mints[rid - 1] if rid and rid <= len(mints) else "unknown"
        value = item.get("result", {}).get("value")
        if value:
            info = value["data"]["parsed"]["info"]
            print(f"  id={rid} mint={mint[:12]}... decimals={info['decimals']} supply={info.get('supply', 'N/A')}")
        else:
            print(f"  id={rid} mint={mint[:12]}... NO DATA")

    return len(results) == len(mints)


if __name__ == "__main__":
    print(f"Solana RPC: {RPC_URL}")
    print()

    ok1 = test_pda_and_metadata()
    ok2 = test_supply_extraction()
    ok3 = test_batch_rpc()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  PDA + Metadata:    {'PASS' if ok1 else 'FAIL'}")
    print(f"  Supply extraction: {'PASS' if ok2 else 'FAIL'}")
    print(f"  Batch RPC:         {'PASS' if ok3 else 'FAIL'}")
    print("=" * 70)

    sys.exit(0 if (ok1 and ok2 and ok3) else 1)
