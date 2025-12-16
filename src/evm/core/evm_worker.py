import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import polars as pl

from ...database import get_evm_db_client, get_postgres_client
from ..config import EvmChainConfig, EvmConfig
from ..processors import (
    EvmDecimalsResolver,
    EvmFirstTxFinder,
    EvmLiquidityProxy,
    EvmPriceCalculator,
    EvmRpcEnricher,
    EvmTokenDiscovery,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvmRunSpec:
    chain: str
    window_start: datetime
    window_end: datetime
    min_swaps: int
    view_source: str


class EvmTokenAggregationWorker:
    def __init__(self, chain_cfg: EvmChainConfig, chunk_size: Optional[int] = None):
        self.db_client = get_evm_db_client()
        self._postgres = None
        self.chain_cfg = chain_cfg
        self.chunk_size = int(chunk_size or EvmConfig.DEFAULT_CHUNK_SIZE)

        self.token_discovery = EvmTokenDiscovery(self.db_client, chain_cfg)
        self.first_tx = EvmFirstTxFinder(self.db_client, chain_cfg)
        self.decimals = EvmDecimalsResolver(self.db_client, chain_cfg)
        self.price = EvmPriceCalculator(self.db_client, chain_cfg)
        self.liquidity = EvmLiquidityProxy(self.db_client, chain_cfg)
        self.rpc = EvmRpcEnricher(chain_cfg)

    def run(self, spec: EvmRunSpec, write_postgres: bool = True) -> pl.DataFrame:
        logger.info("=" * 100)
        logger.info("EVM TOKEN AGGREGATION")
        logger.info(f"Chain: {spec.chain}")
        logger.info(f"Window: {spec.window_start.isoformat()} -> {spec.window_end.isoformat()}")
        logger.info(f"Min swaps: {spec.min_swaps}")
        logger.info(f"Chunk size: {self.chunk_size:,}")
        logger.info("=" * 100)

        tokens = self.token_discovery.discover_tokens(
            window_start=spec.window_start,
            window_end=spec.window_end,
            min_swaps=spec.min_swaps,
        )
        if not tokens:
            logger.warning("No tokens discovered; nothing to do.")
            return pl.DataFrame(
                schema={
                    "mint": pl.Utf8,
                    "chain": pl.Utf8,
                    "symbol": pl.Utf8,
                    "name": pl.Utf8,
                    "decimals": pl.Int64,
                    "price_usd": pl.Float64,
                    "supply": pl.Float64,
                    "market_cap_usd": pl.Float64,
                    "largest_lp_pool_usd": pl.Float64,
                    "first_tx_date": pl.Datetime,
                }
            )

        all_dfs: List[pl.DataFrame] = []
        total_chunks = (len(tokens) + self.chunk_size - 1) // self.chunk_size

        for chunk_idx in range(0, len(tokens), self.chunk_size):
            chunk = tokens[chunk_idx : chunk_idx + self.chunk_size]
            chunk_num = (chunk_idx // self.chunk_size) + 1
            logger.info("-" * 80)
            logger.info(f"Chunk {chunk_num}/{total_chunks}: {len(chunk):,} tokens")

            df = self._process_chunk(
                chain=spec.chain,
                tokens=chunk,
                window_start=spec.window_start,
                window_end=spec.window_end,
            )
            all_dfs.append(df)

        df_all = pl.concat(all_dfs) if all_dfs else pl.DataFrame()
        df_all = df_all.unique(subset=["mint", "chain"], keep="last")

        if write_postgres and len(df_all) > 0:
            logger.info("=" * 100)
            logger.info("WRITING TO POSTGRES (unverified_tokens)")
            logger.info("=" * 100)
            if self._postgres is None:
                self._postgres = get_postgres_client()
            self._postgres.insert_token_metrics_batch(df=df_all, view_source=spec.view_source, batch_size=1000)

        return df_all

    def _process_chunk(self, chain: str, tokens: List[str], window_start: datetime, window_end: datetime) -> pl.DataFrame:
        temp_table_name = "chunk_tokens"
        data = [[t.lower()] for t in tokens]
        self.db_client.manage_chunk_table(temp_table_name, data, column_names=["mint"])

        df = pl.DataFrame({"mint": [t.lower() for t in tokens]})

        df_first = self.first_tx.get_first_tx_for_chunk()
        df = df.join(df_first, on="mint", how="left")

        df_dec = self.decimals.get_decimals_for_chunk()
        df = df.join(df_dec, on="mint", how="left")

        df_price = self.price.get_prices_for_chunk(window_start=window_start, window_end=window_end)
        df = df.join(df_price, on="mint", how="left")

        df_liq = self.liquidity.get_liquidity_proxy_for_chunk()
        df = df.join(df_liq, on="mint", how="left")

        enrich = self.rpc.enrich(tokens=[t.lower() for t in tokens])
        if enrich:
            df_rpc = pl.DataFrame(enrich)
            df = df.join(df_rpc, on="mint", how="left")

        has_decimals_ch = "decimals" in df.columns
        has_decimals_rpc = "decimals_rpc" in df.columns

        if has_decimals_ch and has_decimals_rpc:
            decimals_expr = pl.coalesce([pl.col("decimals"), pl.col("decimals_rpc")]).alias("decimals_final")
        elif has_decimals_ch:
            decimals_expr = pl.col("decimals").alias("decimals_final")
        elif has_decimals_rpc:
            decimals_expr = pl.col("decimals_rpc").alias("decimals_final")
        else:
            decimals_expr = pl.lit(None).cast(pl.Int64).alias("decimals_final")

        cols_to_add = [
            pl.lit(chain).alias("chain"),
            decimals_expr,
            pl.col("symbol_rpc").alias("symbol_final") if "symbol_rpc" in df.columns else pl.lit(None).alias("symbol_final"),
            pl.col("name_rpc").alias("name_final") if "name_rpc" in df.columns else pl.lit(None).alias("name_final"),
        ]
        df = df.with_columns(cols_to_add)

        supply_expr = (
            pl.when(pl.col("total_supply_raw").is_not_null() & pl.col("decimals_final").is_not_null())
            .then(pl.col("total_supply_raw").cast(pl.Float64) / (10.0 ** pl.col("decimals_final").cast(pl.Float64)))
            .otherwise(0.0)
            if "total_supply_raw" in df.columns
            else pl.lit(0.0)
        )
        df = df.with_columns([
            supply_expr.alias("supply"),
            pl.col("price_usd").fill_null(0.0).alias("price_usd") if "price_usd" in df.columns else pl.lit(0.0).alias("price_usd"),
            pl.col("largest_lp_pool_usd").fill_null(0.0).alias("largest_lp_pool_usd") if "largest_lp_pool_usd" in df.columns else pl.lit(0.0).alias("largest_lp_pool_usd"),
        ])

        df = df.with_columns([(pl.col("price_usd") * pl.col("supply")).alias("market_cap_usd")])

        select_cols = [
            pl.col("mint"),
            pl.col("chain"),
            pl.col("decimals_final").cast(pl.Int64).alias("decimals"),
            pl.col("symbol_final").cast(pl.Utf8).alias("symbol"),
            pl.col("name_final").cast(pl.Utf8).alias("name"),
            pl.col("price_usd").cast(pl.Float64),
            pl.col("market_cap_usd").cast(pl.Float64),
            pl.col("supply").cast(pl.Float64),
            pl.col("largest_lp_pool_usd").cast(pl.Float64),
            pl.col("first_tx_date").cast(pl.Datetime) if "first_tx_date" in df.columns else pl.lit(None).cast(pl.Datetime).alias("first_tx_date"),
        ]
        df_out = df.select(select_cols)

        return df_out
