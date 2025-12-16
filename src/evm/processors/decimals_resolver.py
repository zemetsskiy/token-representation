import logging
from typing import List

import polars as pl

from ...config import Config
from ...database import ClickHouseClient
from ..config import EvmChainConfig

logger = logging.getLogger(__name__)


class EvmDecimalsResolver:
    def __init__(self, db_client: ClickHouseClient, chain_cfg: EvmChainConfig):
        self.db_client = db_client
        self.chain_cfg = chain_cfg

    def get_decimals_for_chunk(self) -> pl.DataFrame:
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        chain = self.chain_cfg.chain

        query = f"""
        SELECT
            lowerUTF8(toString(token_address)) AS mint,
            argMax(token_decimals, block_time) AS decimals
        FROM evm.transfer_events
        WHERE chain = %(chain)s
          AND lowerUTF8(toString(token_address)) IN (SELECT mint FROM {temp_db}.chunk_tokens)
        GROUP BY mint
        """

        logger.info(f"[{chain}] Resolving decimals from transfer_events (full history)")
        rows: List[dict] = self.db_client.execute_query_dict(query, parameters={"chain": chain})
        if not rows:
            return pl.DataFrame({"mint": [], "decimals": []}, schema={"mint": pl.Utf8, "decimals": pl.Int64})

        out = []
        for r in rows:
            token = r.get("mint")
            if isinstance(token, bytes):
                token = token.decode("utf-8", errors="ignore").rstrip("\x00")
            token = str(token).rstrip("\x00").lower()
            out.append({"mint": token, "decimals": int(r.get("decimals")) if r.get("decimals") is not None else None})

        return pl.DataFrame(out, schema={"mint": pl.Utf8, "decimals": pl.Int64})
