#!/usr/bin/env python3
"""
Parity check: run the new DecimalsResolver against a sample of mints and
compare the resolved decimals against the production PostgreSQL value
for the same mint. The PG value was written by the previous code path
(per-batch getAccountInfo), so a match across hundreds of mints means
the new getMultipleAccounts path returns equivalent data.

Usage:
    SOLANA_HTTP_RPC_URL=http://...:8899 \
    POSTGRES_CONNECTION_STRING="postgresql://user:pass@host:5432/default" \
        python3 test_decimals_parity.py [N]
"""
import os
import sys
import time
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import psycopg2
from src.solana.processors.decimals_resolver import DecimalsResolver  # noqa: E402


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 500

    pg_url = os.environ.get('POSTGRES_CONNECTION_STRING', '')
    if not pg_url:
        print('ERROR: POSTGRES_CONNECTION_STRING env var not set', file=sys.stderr)
        sys.exit(2)

    if not os.environ.get('SOLANA_HTTP_RPC_URL'):
        print('ERROR: SOLANA_HTTP_RPC_URL env var not set', file=sys.stderr)
        sys.exit(2)

    print(f'Loading {n} mints+decimals from PG...')
    conn = psycopg2.connect(pg_url)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT contract_address, decimals
            FROM unverified_tokens
            WHERE chain = 'solana' AND decimals IS NOT NULL
            ORDER BY random()
            LIMIT %s
        """, (n,))
        rows = cur.fetchall()
    conn.close()

    pg_map = {addr: dec for addr, dec in rows}
    mints = list(pg_map.keys())
    print(f'Got {len(mints)} mints from PG.')

    resolver = DecimalsResolver()
    # Clear hardcoded anchors so we actually exercise the RPC for them too
    for k in ['So11111111111111111111111111111111111111112',
              'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
              'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB']:
        resolver.decimals_cache.pop(k, None)

    t0 = time.time()
    rpc_map = resolver.resolve_decimals_batch(mints)
    elapsed = time.time() - t0

    match = 0
    mismatch = 0
    rpc_missing = 0
    mismatches: list[tuple[str, int, int]] = []
    for mint, pg_dec in pg_map.items():
        rpc_dec = rpc_map.get(mint)
        if rpc_dec is None:
            rpc_missing += 1
            continue
        if int(rpc_dec) == int(pg_dec):
            match += 1
        else:
            mismatch += 1
            mismatches.append((mint, pg_dec, rpc_dec))

    print('')
    print('=' * 80)
    print('PARITY RESULTS')
    print('=' * 80)
    print(f'Total compared:  {len(pg_map)}')
    print(f'Match:           {match}')
    print(f'Mismatch:        {mismatch}')
    print(f'RPC missing:     {rpc_missing}')
    print(f'Elapsed:         {elapsed:.2f}s')
    if mismatches:
        print('Sample mismatches (mint, pg, rpc):')
        for m, p, r in mismatches[:10]:
            print(f'  {m} pg={p} rpc={r}')

    sys.exit(0 if mismatch == 0 else 1)


if __name__ == '__main__':
    main()
