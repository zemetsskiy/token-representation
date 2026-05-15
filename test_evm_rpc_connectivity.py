#!/usr/bin/env python3
"""
EVM RPC connectivity + decode smoke test.

For each configured chain (eth, bsc, base, polygon), opens the production
EvmRpcClient against the URL in $EVM_RPC_URL_<CHAIN> and exercises:

  1. eth_chainId  — confirms the endpoint speaks JSON-RPC and is the right network.
  2. eth_blockNumber — sanity check, returns a non-zero recent block.
  3. get_token_metadata_batch on one well-known token per chain — confirms
     batched eth_call + ABI-decode round-trip works end to end. This is the
     exact call path that token-evm-100-30d uses and that previously failed
     with ConnectTimeoutError on BSC.

Usage:
    EVM_RPC_URL_ETH=...
    EVM_RPC_URL_BSC=...
    EVM_RPC_URL_BASE=...
    EVM_RPC_URL_POLYGON=...   python3 test_evm_rpc_connectivity.py [chain ...]

If chains are passed positionally, only those run; otherwise all four.
"""
import os
import sys
import time
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import requests  # noqa: E402

from src.evm.rpc.evm_rpc_client import EvmRpcClient  # noqa: E402


# Expected chainId (decimal) per chain.
EXPECTED_CHAIN_ID = {
    'eth': 1,
    'bsc': 56,
    'base': 8453,
    'polygon': 137,
}

# One well-known ERC20 per chain with stable on-chain decimals.
# Symbol is checked only for non-emptiness because issuers occasionally rename
# the symbol on chain (e.g. MATIC -> POL, USDT -> USDT0). Decimals is what the
# worker actually relies on for price/supply math, so we assert that strictly.
WELL_KNOWN = {
    'eth':     ('0xdAC17F958D2ee523a2206206994597C13D831ec7', 6),     # Tether USD
    'bsc':     ('0x55d398326f99059fF775485246999027B3197955', 18),    # Binance-Peg USDT
    'base':    ('0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 6),     # Native USDC
    'polygon': ('0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270', 18),    # Wrapped POL (ex-WMATIC)
}


def _rpc_post(url: str, payload: dict, timeout: float = 5.0) -> dict:
    resp = requests.post(url, json=payload, timeout=timeout,
                         headers={'Content-Type': 'application/json'})
    resp.raise_for_status()
    return resp.json()


def test_chain(chain: str) -> bool:
    url = os.environ.get(f'EVM_RPC_URL_{chain.upper()}', '')
    if not url:
        print(f'[{chain}] SKIP — EVM_RPC_URL_{chain.upper()} not set')
        return True

    print(f'[{chain}] {url}')

    # 1. chainId
    try:
        r = _rpc_post(url, {'jsonrpc': '2.0', 'id': 1, 'method': 'eth_chainId', 'params': []})
    except Exception as e:
        print(f'  chainId: FAIL — {type(e).__name__}: {e}')
        return False
    got = int(r.get('result', '0x0'), 16) if isinstance(r.get('result'), str) else None
    want = EXPECTED_CHAIN_ID[chain]
    if got != want:
        print(f'  chainId: FAIL — got {got}, expected {want}')
        return False
    print(f'  chainId:        {got} (ok)')

    # 2. blockNumber
    try:
        r = _rpc_post(url, {'jsonrpc': '2.0', 'id': 1, 'method': 'eth_blockNumber', 'params': []})
    except Exception as e:
        print(f'  blockNumber: FAIL — {type(e).__name__}: {e}')
        return False
    blk = int(r.get('result', '0x0'), 16)
    if blk == 0:
        print(f'  blockNumber: FAIL — node reports block 0 (not synced)')
        return False
    print(f'  blockNumber:    {blk:,}')

    # 3. ERC20 metadata decode via EvmRpcClient
    addr, want_decimals = WELL_KNOWN[chain]
    client = EvmRpcClient(chain, url)
    t0 = time.time()
    try:
        meta = client.get_token_metadata_batch([addr])
    except Exception as e:
        print(f'  erc20 metadata: FAIL — {type(e).__name__}: {e}')
        return False
    elapsed = time.time() - t0

    got = meta.get(addr.lower(), {})
    sym = got.get('symbol')
    dec = got.get('decimals')
    name = got.get('name')
    if dec != want_decimals:
        print(f'  erc20 metadata: FAIL — got decimals={dec} (expected {want_decimals})')
        return False
    if not sym:
        print(f'  erc20 metadata: FAIL — empty symbol (decode broken?)')
        return False
    print(f'  erc20 metadata: symbol={sym!r} decimals={dec} name={name!r} ({elapsed*1000:.0f} ms)')
    return True


def main() -> int:
    chains = sys.argv[1:] or ['eth', 'bsc', 'base', 'polygon']
    for c in chains:
        if c not in EXPECTED_CHAIN_ID:
            print(f'ERROR: unknown chain {c!r}', file=sys.stderr)
            return 2

    failed = []
    for c in chains:
        ok = test_chain(c)
        if not ok:
            failed.append(c)
        print()

    print('=' * 60)
    if failed:
        print(f'FAIL: {", ".join(failed)}')
        return 1
    print(f'OK ({len(chains)} chain{"s" if len(chains) != 1 else ""})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
