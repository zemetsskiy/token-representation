#!/usr/bin/env python3
"""
EVM pipeline parity/semantics checks for Postgres UPSERT behavior.

This is intentionally ClickHouse/RPC-independent. It validates:
  - The DataFrame schema matches what the pipeline writes.
  - Postgres upsert preserves `decimals` and `first_tx_date` once set.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from src.config import setup_logging  # noqa: E402
from src.database import get_postgres_client  # noqa: E402


setup_logging()


CANON_COLS = [
    "mint",
    "chain",
    "decimals",
    "symbol",
    "name",
    "price_usd",
    "market_cap_usd",
    "supply",
    "largest_lp_pool_usd",
    "first_tx_date",
]


def ensure_unverified_tokens_table(pg) -> None:
    # Minimal safety: create table + view if missing (matches create_unverified_tokens_clean.sql contract).
    pg.cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unverified_tokens (
            id BIGSERIAL PRIMARY KEY,
            contract_address VARCHAR(48) NOT NULL,
            chain VARCHAR(50) NOT NULL,
            decimals INTEGER,
            symbol VARCHAR(20),
            name VARCHAR(255),
            price_usd DOUBLE PRECISION DEFAULT 0,
            market_cap_usd DOUBLE PRECISION DEFAULT 0,
            supply DOUBLE PRECISION DEFAULT 0,
            largest_lp_pool_usd DOUBLE PRECISION DEFAULT 0,
            first_tx_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            view_source VARCHAR(100),
            CONSTRAINT unique_token_chain UNIQUE (contract_address, chain)
        );
        """
    )
    pg.cursor.execute(
        """
        CREATE OR REPLACE VIEW latest_unverified_tokens AS
        SELECT DISTINCT ON (contract_address, chain)
            id, contract_address, chain, decimals, symbol, name, price_usd, market_cap_usd, supply,
            largest_lp_pool_usd, first_tx_date, created_at, updated_at, view_source
        FROM unverified_tokens
        ORDER BY contract_address, chain, updated_at DESC;
        """
    )
    pg.connection.commit()


def main() -> int:
    pg = get_postgres_client()
    ensure_unverified_tokens_table(pg)

    token = "0x1111111111111111111111111111111111111111"
    chain = "eth"
    t0 = datetime.utcnow().replace(microsecond=0) - timedelta(days=10)
    t1 = t0 + timedelta(days=5)

    # Insert baseline
    df1 = pl.DataFrame(
        [
            {
                "mint": token,
                "chain": chain,
                "decimals": 18,
                "symbol": "TOK",
                "name": "Token One",
                "price_usd": 1.0,
                "market_cap_usd": 100.0,
                "supply": 100.0,
                "largest_lp_pool_usd": 10.0,
                "first_tx_date": t0,
            }
        ]
    )

    missing = [c for c in CANON_COLS if c not in df1.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    pg.insert_token_metrics_batch(df=df1, view_source="test_evm_semantics_v1", batch_size=1000)

    # Attempt to overwrite decimals and first_tx_date (should NOT overwrite)
    df2 = pl.DataFrame(
        [
            {
                "mint": token,
                "chain": chain,
                "decimals": 6,  # should be ignored
                "symbol": "TOK2",
                "name": "Token Two",
                "price_usd": 2.0,
                "market_cap_usd": 200.0,
                "supply": 100.0,
                "largest_lp_pool_usd": 20.0,
                "first_tx_date": t1,  # should be ignored
            }
        ]
    )
    pg.insert_token_metrics_batch(df=df2, view_source="test_evm_semantics_v2", batch_size=1000)

    latest = pg.get_latest_metrics(contract_address=token, chain=chain)
    if not latest:
        raise RuntimeError("Did not find token in latest_unverified_tokens")

    # Preservation checks
    assert int(latest["decimals"]) == 18, f"decimals overwritten: {latest['decimals']}"
    assert latest["first_tx_date"] == t0, f"first_tx_date overwritten: {latest['first_tx_date']} != {t0}"

    # Update checks
    assert latest["symbol"] == "TOK2"
    assert latest["name"] == "Token Two"
    assert float(latest["price_usd"]) == 2.0
    assert float(latest["largest_lp_pool_usd"]) == 20.0
    assert latest["view_source"] == "test_evm_semantics_v2"

    print("✅ EVM Postgres upsert semantics validated (decimals/first_tx_date preserved).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())







