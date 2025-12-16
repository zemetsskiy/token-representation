#!/usr/bin/env python3
"""
EVM Token Worker for Scheduled Processing.

This is a parallel entrypoint to `token_representation/worker_scheduled.py` (Solana).
It ingests EVM token representation data over a configurable time window, per chain,
and upserts into PostgreSQL `unverified_tokens` using the shared Postgres client.

Usage with predefined views:
  --view evm_1000_swaps_3_days   : 1000+ swaps in 3 days  (run every 3h)
  --view evm_500_swaps_7_days    : 500+ swaps in 7 days   (run every 12h)
  --view evm_100_swaps_30_days   : 100+ swaps in 30 days  (run daily)

Or manual configuration via --window-days and --min-swaps.
"""

import argparse
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple, Optional

project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from src.config import setup_logging  # noqa: E402
from src.database import get_evm_db_client  # noqa: E402
from src.evm.config import EvmConfig  # noqa: E402
from src.evm.core import EvmTokenAggregationWorker  # noqa: E402
from src.evm.core.evm_worker import EvmRunSpec  # noqa: E402

setup_logging()
logger = logging.getLogger(__name__)


class ViewConfig(NamedTuple):
    """Configuration for a predefined view."""
    name: str
    min_swaps: int
    window_days: int
    description: str


# Predefined views for scheduled runs (naming: evm_<swaps>_swaps_<days>_days)
VIEWS = {
    "evm_1000_swaps_3_days": ViewConfig(
        name="evm_1000_swaps_3_days",
        min_swaps=1000,
        window_days=3,
        description="High activity tokens (1000+ swaps in 3 days)",
    ),
    "evm_500_swaps_7_days": ViewConfig(
        name="evm_500_swaps_7_days",
        min_swaps=500,
        window_days=7,
        description="Medium activity tokens (500+ swaps in 7 days)",
    ),
    "evm_100_swaps_30_days": ViewConfig(
        name="evm_100_swaps_30_days",
        min_swaps=100,
        window_days=30,
        description="Low activity tokens (100+ swaps in 30 days)",
    ),
}


def parse_view_name(view_name: str) -> Optional[ViewConfig]:
    """
    Parse view name to extract config. Supports predefined views or pattern:
    evm_<swaps>_swaps_<days>_days (e.g., evm_200_swaps_14_days)
    """
    # Check predefined views first
    if view_name in VIEWS:
        return VIEWS[view_name]

    # Try to parse custom pattern: evm_<swaps>_swaps_<days>_days
    match = re.match(r"evm_(\d+)_swaps_(\d+)_days?", view_name)
    if match:
        min_swaps = int(match.group(1))
        window_days = int(match.group(2))
        return ViewConfig(
            name=view_name,
            min_swaps=min_swaps,
            window_days=window_days,
            description=f"Custom view ({min_swaps}+ swaps in {window_days} days)",
        )

    return None


def _parse_dt(value: str) -> datetime:
    # Accept ISO strings; if naive, assume UTC.
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _check_evm_data_availability(db) -> dict:
    """Check what EVM data is available in ClickHouse."""
    info = {"swap_count": 0, "earliest": None, "latest": None, "chains": []}
    try:
        result = db.execute_query_dict("SELECT count() as cnt FROM evm.swap_events")
        info["swap_count"] = result[0]["cnt"] if result else 0
        if info["swap_count"] > 0:
            dates = db.execute_query_dict(
                "SELECT min(block_time) as mn, max(block_time) as mx FROM evm.swap_events"
            )
            if dates:
                info["earliest"] = dates[0]["mn"]
                info["latest"] = dates[0]["mx"]
            chains = db.execute_query_dict("SELECT DISTINCT chain FROM evm.swap_events")
            for c in chains:
                chain = c["chain"]
                if isinstance(chain, bytes):
                    chain = chain.decode().strip("\x00")
                info["chains"].append(str(chain).strip("\x00"))
    except Exception:
        pass
    return info


def main():
    parser = argparse.ArgumentParser(
        description="EVM Token Representation Scheduled Worker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Predefined views (use --view):
  evm_1000_swaps_3_days   : 1000+ swaps in 3 days   (recommended: run every 3h)
  evm_500_swaps_7_days    : 500+ swaps in 7 days    (recommended: run every 12h)
  evm_100_swaps_30_days   : 100+ swaps in 30 days   (recommended: run daily)

Custom views are also supported:
  evm_<swaps>_swaps_<days>_days  (e.g., evm_200_swaps_14_days)

Examples:
  python evm_worker_scheduled.py --view evm_1000_swaps_3_days --chains eth
  python evm_worker_scheduled.py --view evm_100_swaps_30_days --dry-run
  python evm_worker_scheduled.py --view evm_200_swaps_14_days
        """,
    )
    parser.add_argument("--view", type=str, required=True, help="View name (e.g., evm_500_swaps_7_days)")
    parser.add_argument("--chains", type=str, default=None, help="Comma-separated chain list (default: all configured)")
    parser.add_argument("--from", dest="from_dt", type=str, default=None, help="ISO datetime UTC start (overrides window)")
    parser.add_argument("--to", dest="to_dt", type=str, default=None, help="ISO datetime UTC end (default: now)")
    parser.add_argument("--chunk-size", type=int, default=EvmConfig.DEFAULT_CHUNK_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write to Postgres")
    args = parser.parse_args()

    # Parse view configuration
    view_config = parse_view_name(args.view)
    if not view_config:
        logger.error(f"Invalid view name: {args.view}")
        logger.error("Expected format: evm_<swaps>_swaps_<days>_days (e.g., evm_500_swaps_7_days)")
        logger.error(f"Predefined views: {', '.join(VIEWS.keys())}")
        raise SystemExit(1)

    view_name = view_config.name
    window_days = view_config.window_days
    min_swaps = view_config.min_swaps
    logger.info(f"Using view '{view_name}': {view_config.description}")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    window_end = _parse_dt(args.to_dt) if args.to_dt else now
    window_start = _parse_dt(args.from_dt) if args.from_dt else (window_end - timedelta(days=window_days))

    db = get_evm_db_client()
    ch_chains = EvmConfig.chains_in_clickhouse(db, window_start, window_end)

    chain_cfgs = EvmConfig.default_chain_configs()
    if args.chains:
        wanted = {c.strip() for c in args.chains.split(",") if c.strip()}
    else:
        # Default: all chains present in ClickHouse, as requested.
        wanted = set(ch_chains)

    chains = [c for c in sorted(wanted)]
    if not chains:
        # Provide helpful diagnostic info
        info = _check_evm_data_availability(db)
        logger.error("=" * 80)
        logger.error("NO CHAINS FOUND")
        logger.error("=" * 80)
        logger.error(f"Requested window: {window_start.isoformat()} -> {window_end.isoformat()}")
        if info["swap_count"] == 0:
            logger.error("evm.swap_events table is EMPTY - no EVM data has been loaded yet.")
            logger.error("Please ensure EVM swap data is ingested into ClickHouse before running this worker.")
        else:
            logger.error(f"evm.swap_events has {info['swap_count']:,} rows")
            logger.error(f"Available data range: {info['earliest']} -> {info['latest']}")
            logger.error(f"Available chains: {', '.join(info['chains']) if info['chains'] else 'none'}")
            logger.error("Try adjusting --from/--to or --window-days to match available data.")
        logger.error("=" * 80)
        raise SystemExit(1)

    logger.info("=" * 80)
    logger.info(f"EVM WORKER: view={view_name}, window={window_days}d, min_swaps={min_swaps}")
    logger.info(f"Window: {window_start.isoformat()} -> {window_end.isoformat()}")
    logger.info(f"Chains: {', '.join(chains)}")
    logger.info("=" * 80)

    for chain in chains:
        cfg = chain_cfgs.get(chain)
        if not cfg:
            logger.warning(
                f"[{chain}] Skipping: chain is present in ClickHouse but no RPC URL/config is provided. "
                "Set EVM_RPC_URL_<CHAIN> or EVM_CHAIN_CONFIGS."
            )
            continue
        worker = EvmTokenAggregationWorker(chain_cfg=cfg, chunk_size=args.chunk_size)
        # view_source format: <view_name>_<chain> (e.g., evm_500_swaps_7_days_eth)
        view_source = f"{view_name}_{chain}"
        spec = EvmRunSpec(
            chain=chain,
            window_start=window_start,
            window_end=window_end,
            min_swaps=min_swaps,
            view_source=view_source,
        )
        df = worker.run(spec, write_postgres=not args.dry_run)
        logger.info(f"[{chain}] Done. Rows: {len(df):,}")


if __name__ == "__main__":
    main()


