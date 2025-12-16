import logging

import polars as pl

from ...database import ClickHouseClient
from ..config import EvmChainConfig

logger = logging.getLogger(__name__)


class EvmLiquidityProxy:
    def __init__(self, db_client: ClickHouseClient, chain_cfg: EvmChainConfig):
        self.db_client = db_client
        self.chain_cfg = chain_cfg

    def get_liquidity_proxy_for_chunk(self) -> pl.DataFrame:
        logger.info(f"[{self.chain_cfg.chain}] Liquidity proxy: returning NULL (not implemented for EVM)")
        return pl.DataFrame({"mint": [], "largest_lp_pool_usd": []}, schema={"mint": pl.Utf8, "largest_lp_pool_usd": pl.Float64})
