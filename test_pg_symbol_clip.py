#!/usr/bin/env python3
"""
PG upsert robustness test for the symbol/name length fix.

Exercises src.database.postgres against the live production PG, asserting:
  1. The schema is migrated (symbol >= varchar(64)).
  2. _clip_text behaves correctly on edge cases.
  3. Upserting rows with over-long symbol and name does NOT raise
     StringDataRightTruncation, and the persisted values are clipped to the
     defensive caps (64 / 255).
  4. The test rows are removed at the end so we don't pollute the table.

Usage:
    POSTGRES_CONNECTION_STRING="postgresql://user:pass@host:5432/default" \
        python3 test_pg_symbol_clip.py
"""
import os
import sys
import secrets
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import psycopg2
import polars as pl

# Make sure the test imports the production code path under test.
from src.database.postgres import (  # noqa: E402
    PostgresClient,
    _clip_text,
    SYMBOL_MAX_LEN,
    NAME_MAX_LEN,
)


TEST_CHAIN = 'solana'
# Sentinel prefix so we can identify and clean up our test rows.
TEST_TAG = '__pgcliptest__'
TEST_VIEW_SOURCE = TEST_TAG


def _make_addr() -> str:
    """Generate a fake 44-char base58-ish mint address unique to this test run."""
    return TEST_TAG + secrets.token_hex(8)  # 14 (tag) + 16 (hex) = 30 chars; fits in varchar(48)


def _cleanup(conn_str: str) -> int:
    """Remove rows from prior test runs. Returns number deleted."""
    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM unverified_tokens WHERE chain = %s AND contract_address LIKE %s",
                (TEST_CHAIN, TEST_TAG + '%'),
            )
            n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def test_clip_text() -> None:
    """Unit-style assertions for the clip helper itself."""
    assert _clip_text(None, 10) is None
    assert _clip_text('', 10) is None
    assert _clip_text('   ', 10) is None, 'whitespace-only must collapse to None'
    assert _clip_text('abc', 10) == 'abc'
    assert _clip_text(' abc ', 10) == 'abc', 'must strip whitespace'
    assert _clip_text('a' * 30, 10) == 'a' * 10
    # NUL byte handling
    assert _clip_text('ab\x00cd', 10) == 'abcd'
    # Unicode: char count, not byte count
    s = 'ё' * 50  # 50 cyrillic chars (each 2 bytes in UTF-8)
    assert _clip_text(s, 20) == 'ё' * 20
    print('  test_clip_text: PASS')


def test_schema_widened(conn_str: str) -> None:
    """Verify the production schema has symbol >= varchar(64)."""
    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT character_maximum_length FROM information_schema.columns
                   WHERE table_name='unverified_tokens' AND column_name='symbol'"""
            )
            (sym_len,) = cur.fetchone()
            cur.execute(
                """SELECT character_maximum_length FROM information_schema.columns
                   WHERE table_name='unverified_tokens' AND column_name='name'"""
            )
            (name_len,) = cur.fetchone()
    finally:
        conn.close()
    assert sym_len >= SYMBOL_MAX_LEN, f'symbol column is varchar({sym_len}), expected >= {SYMBOL_MAX_LEN}'
    assert name_len >= NAME_MAX_LEN, f'name column is varchar({name_len}), expected >= {NAME_MAX_LEN}'
    print(f'  test_schema_widened: PASS (symbol=varchar({sym_len}), name=varchar({name_len}))')


def test_upsert_long_strings(conn_str: str) -> None:
    """Insert rows with symbol/name longer than the column caps.

    Pre-fix this raised psycopg2.errors.StringDataRightTruncation.
    Post-fix the app clips defensively and the upsert succeeds.
    """
    addr1 = _make_addr()
    addr2 = _make_addr()
    # 200-char symbol (well past varchar(64)) and 1000-char name (well past varchar(255)).
    long_symbol = 'X' * 200
    long_name = 'N' * 1000
    df = pl.DataFrame({
        'mint': [addr1, addr2],
        'chain': [TEST_CHAIN, TEST_CHAIN],
        'decimals': [9, 6],
        'symbol': [long_symbol, 'ok'],
        'name': [long_name, 'ok name'],
        'price_usd': [1.0, 2.0],
        'market_cap_usd': [10.0, 20.0],
        'supply': [100.0, 200.0],
        'largest_lp_pool_usd': [0.0, 0.0],
        'total_liquidity_usd': [0.0, 0.0],
        'last_trade_time': [None, None],
        'first_tx_date': [None, None],
    })

    client = PostgresClient()
    try:
        inserted = client.insert_token_metrics_batch(df, view_source=TEST_VIEW_SOURCE)
    finally:
        # Best-effort close.
        try:
            if client.connection:
                client.connection.close()
        except Exception:
            pass

    assert inserted == 2, f'expected 2 rows upserted, got {inserted}'

    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT contract_address, symbol, name FROM unverified_tokens "
                "WHERE chain = %s AND contract_address IN (%s, %s)",
                (TEST_CHAIN, addr1, addr2),
            )
            rows = {addr: (sym, name) for addr, sym, name in cur.fetchall()}
    finally:
        conn.close()

    assert addr1 in rows and addr2 in rows, f'persisted rows: {sorted(rows)}'
    sym_persisted, name_persisted = rows[addr1]
    assert len(sym_persisted) == SYMBOL_MAX_LEN, (
        f'symbol persisted len={len(sym_persisted)}, expected exactly {SYMBOL_MAX_LEN}'
    )
    assert sym_persisted == 'X' * SYMBOL_MAX_LEN
    assert len(name_persisted) == NAME_MAX_LEN, (
        f'name persisted len={len(name_persisted)}, expected exactly {NAME_MAX_LEN}'
    )
    sym2, name2 = rows[addr2]
    assert sym2 == 'ok' and name2 == 'ok name', f'normal row mangled: {sym2!r} / {name2!r}'
    print('  test_upsert_long_strings: PASS '
          f'(addr1 symbol clipped to {SYMBOL_MAX_LEN}, name clipped to {NAME_MAX_LEN}; normal row intact)')


def main() -> int:
    conn_str = os.environ.get('POSTGRES_CONNECTION_STRING', '')
    if not conn_str:
        print('ERROR: POSTGRES_CONNECTION_STRING env var not set', file=sys.stderr)
        return 2

    print('Cleaning up any stale test rows...')
    n = _cleanup(conn_str)
    if n:
        print(f'  removed {n} stale rows')

    failed = 0
    try:
        test_clip_text()
    except AssertionError as e:
        print(f'  test_clip_text: FAIL — {e}')
        failed += 1

    try:
        test_schema_widened(conn_str)
    except AssertionError as e:
        print(f'  test_schema_widened: FAIL — {e}')
        failed += 1

    try:
        test_upsert_long_strings(conn_str)
    except AssertionError as e:
        print(f'  test_upsert_long_strings: FAIL — {e}')
        failed += 1
    except Exception as e:
        print(f'  test_upsert_long_strings: ERROR — {type(e).__name__}: {e}')
        failed += 1

    print('Final cleanup...')
    try:
        n = _cleanup(conn_str)
        print(f'  removed {n} rows')
    except Exception as e:
        print(f'  cleanup failed: {e}')

    print('=' * 60)
    print('OK' if failed == 0 else f'FAIL ({failed} failure(s))')
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
