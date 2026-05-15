#!/usr/bin/env python3
"""
Regression test for the polars-overflow bug on huge ERC20 totalSupply.

Before the fix, EvmRpcEnricher.enrich() returned `total_supply_raw` as a
Python int. Some BSC scam/meme tokens declare totalSupply > 2^63 (the
observed prod failure was 10^42). Feeding that list of dicts to
pl.DataFrame() made polars infer Int64 and crash with:

    OverflowError: int value too large for Polars integer types: ...

After the fix the enricher returns it as Python float, which polars treats
as Float64 — no overflow, downstream supply/decimals math is unchanged.

Usage:
    python3 test_evm_supply_overflow.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import polars as pl  # noqa: E402

from src.evm.processors.rpc_enricher import EvmRpcEnricher  # noqa: E402


HUGE_SUPPLY = 10 ** 42  # the exact magnitude seen on BSC in prod


class _StubRpc:
    """Stand-in for EvmRpcClient — never touches the network."""
    def __init__(self):
        # Pretend two tokens already came back from get_token_metadata_batch /
        # get_total_supply_batch. One has a sane uint64 supply, the other has
        # the absurd 10^42 supply that broke the worker in prod.
        self._meta_cache = {
            '0xnormal': {'decimals': 18, 'symbol': 'OK',   'name': 'Normal'},
            '0xhuge':   {'decimals': 18, 'symbol': 'HUGE', 'name': 'HugeToken'},
        }
        self._supply_cache = {
            '0xnormal': 1_000_000 * (10 ** 18),  # 1M tokens, fits in Int64
            '0xhuge':   HUGE_SUPPLY,             # blows out Int64
        }

    def enrich_tokens_parallel(self, tokens):
        # Returns whatever's already in cache; enrich() shouldn't call us
        # because both tokens are pre-loaded.
        return {}, {}


def _make_enricher() -> EvmRpcEnricher:
    enricher = EvmRpcEnricher.__new__(EvmRpcEnricher)  # bypass __init__
    enricher.chain_cfg = type('cfg', (), {'chain': 'bsc'})()
    enricher.rpc = _StubRpc()
    enricher._meta_cache = enricher.rpc._meta_cache
    enricher._supply_cache = enricher.rpc._supply_cache
    return enricher


def test_supply_is_float() -> None:
    """Enricher output must report supply as float so polars infers Float64."""
    enricher = _make_enricher()
    rows = enricher.enrich(['0xnormal', '0xhuge'])
    assert len(rows) == 2
    huge_row = next(r for r in rows if r['mint'] == '0xhuge')
    sup = huge_row['total_supply_raw']
    assert isinstance(sup, float), f'expected float, got {type(sup).__name__}'
    assert sup == float(HUGE_SUPPLY), f'value lost in cast: {sup}'
    print('  test_supply_is_float: PASS '
          f'(huge supply {sup:.3e} returned as Python float)')


def test_polars_dataframe_construction() -> None:
    """The actual crash path: pl.DataFrame(enricher.enrich(...))."""
    enricher = _make_enricher()
    rows = enricher.enrich(['0xnormal', '0xhuge'])
    # This is the line that used to raise OverflowError in evm_worker.py:146.
    df = pl.DataFrame(rows)
    assert df.height == 2
    assert df.schema['total_supply_raw'] == pl.Float64, (
        f'wrong dtype: {df.schema["total_supply_raw"]}'
    )
    # Replicate the downstream computation that normalizes supply by decimals.
    df = df.with_columns(
        (pl.col('total_supply_raw').cast(pl.Float64) /
         (10.0 ** pl.col('decimals_rpc').cast(pl.Float64))).alias('supply')
    )
    huge = df.filter(pl.col('mint') == '0xhuge').row(0, named=True)
    # 10^42 / 10^18 = 10^24, well within Float64 range
    assert huge['supply'] == 1e24, f'unexpected normalized supply: {huge["supply"]}'
    print('  test_polars_dataframe_construction: PASS '
          f'(DataFrame built, dtype=Float64, supply normalized to {huge["supply"]:.3e})')


def test_none_supply_preserved() -> None:
    """If RPC returns None (eth_call reverted), the enricher must propagate None."""
    enricher = _make_enricher()
    enricher._supply_cache['0xnormal'] = None
    rows = enricher.enrich(['0xnormal'])
    assert rows[0]['total_supply_raw'] is None
    print('  test_none_supply_preserved: PASS')


def main() -> int:
    failed = 0
    for test in (test_supply_is_float,
                 test_polars_dataframe_construction,
                 test_none_supply_preserved):
        try:
            test()
        except Exception as e:
            print(f'  {test.__name__}: FAIL — {type(e).__name__}: {e}')
            failed += 1
    print('=' * 60)
    print('OK' if failed == 0 else f'FAIL ({failed} failure(s))')
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
