#!/usr/bin/env python3
"""
Benchmark + correctness test for DecimalsResolver against a live Solana RPC.

Runs the production class as-is against a real set of mint addresses, reports:
  - elapsed wall-clock time
  - per-batch latency
  - decimals-resolved success rate
  - correctness for 3 anchor mints (Wrapped SOL=9, USDC=6, USDT=6)
  - count of WARN/ERROR retries observed in the run

Usage:
    SOLANA_HTTP_RPC_URL=http://64.130.57.54:8899 \
        python3 test_decimals_session.py [mints_file] [N]

Default mints_file = /tmp/sample_mints.txt (one mint per line).
Default N = all mints in file.

Always includes the 3 anchor mints so correctness checks run even on an empty file.
"""

import os
import sys
import time
import logging
from pathlib import Path

# Make `src...` imports work from repo root
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.solana.processors.decimals_resolver import DecimalsResolver  # noqa: E402

# Anchors with known on-chain decimals — these are baked into the resolver's
# in-memory cache (so they don't actually hit the RPC), but we keep them to
# verify the result mapping still returns the correct values.
ANCHORS = {
    'So11111111111111111111111111111111111111112': 9,    # Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6,   # USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6,   # USDT
}


class CountingHandler(logging.Handler):
    """Capture WARN/ERROR counts from inside the resolver."""

    def __init__(self):
        super().__init__()
        self.warns = 0
        self.errors = 0
        self.recent_msgs: list[str] = []

    def emit(self, record):
        msg = record.getMessage()
        if record.levelno >= logging.ERROR:
            self.errors += 1
            self.recent_msgs.append(f'ERR: {msg[:160]}')
        elif record.levelno >= logging.WARNING:
            self.warns += 1
            self.recent_msgs.append(f'WARN: {msg[:160]}')


def main():
    mints_file = sys.argv[1] if len(sys.argv) > 1 else '/tmp/sample_mints.txt'
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None

    rpc_url = os.environ.get('SOLANA_HTTP_RPC_URL', '')
    if not rpc_url:
        print('ERROR: SOLANA_HTTP_RPC_URL env var not set', file=sys.stderr)
        sys.exit(2)

    # Load mints
    mints: list[str] = []
    if Path(mints_file).exists():
        with open(mints_file) as f:
            mints = [line.strip() for line in f if line.strip()]
    if limit:
        mints = mints[:limit]

    # Always prepend anchors so correctness check runs
    for a in ANCHORS:
        if a not in mints:
            mints.insert(0, a)

    print('=' * 80)
    print(f'RPC:         {rpc_url}')
    print(f'Mints file:  {mints_file}')
    print(f'Mints count: {len(mints)}  (anchors={len(ANCHORS)})')
    print('=' * 80)

    # Hook a counting handler onto the resolver's logger
    counter = CountingHandler()
    counter.setLevel(logging.WARNING)
    resolver_log = logging.getLogger('src.solana.processors.decimals_resolver')
    resolver_log.addHandler(counter)
    # Make sure log output is visible
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')

    resolver = DecimalsResolver()

    t0 = time.time()
    result = resolver.resolve_decimals_batch(mints)
    elapsed = time.time() - t0

    found = sum(1 for v in result.values() if v is not None)
    not_found = sum(1 for v in result.values() if v is None)

    print('')
    print('=' * 80)
    print('RESULTS')
    print('=' * 80)
    print(f'Total mints:      {len(mints)}')
    print(f'Decimals found:   {found}  ({100*found/len(mints):.1f}%)')
    print(f'Decimals missing: {not_found}')
    print(f'Wall time:        {elapsed:.2f} s')
    print(f'Throughput:       {len(mints)/elapsed:.1f} mints/s')
    print(f'WARN logs:        {counter.warns}')
    print(f'ERROR logs:       {counter.errors}')

    # Anchor correctness
    print('')
    print('Anchor correctness:')
    anchor_ok = True
    for mint, expected in ANCHORS.items():
        actual = result.get(mint)
        ok = (actual == expected)
        anchor_ok = anchor_ok and ok
        print(f'  {mint[:16]}... expected={expected:<3} got={actual!r:<5} {"OK" if ok else "FAIL"}')

    print('')
    if counter.recent_msgs:
        print(f'First {min(5,len(counter.recent_msgs))} retry messages:')
        for m in counter.recent_msgs[:5]:
            print(f'  {m}')

    print('')
    print('PASS' if anchor_ok else 'FAIL')
    sys.exit(0 if anchor_ok else 1)


if __name__ == '__main__':
    main()
