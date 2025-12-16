import logging
from typing import List

import polars as pl

from ...config import Config
from ...database import ClickHouseClient
from ..config import EvmChainConfig

logger = logging.getLogger(__name__)


class EvmFirstTxFinder:
    def __init__(self, db_client: ClickHouseClient, chain_cfg: EvmChainConfig):
        self.db_client = db_client
        self.chain_cfg = chain_cfg

    def get_first_tx_for_chunk(self) -> pl.DataFrame:
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        chain = self.chain_cfg.chain

        query = f"""
        WITH tokens AS (
            SELECT mint AS token
            FROM {temp_db}.chunk_tokens
        ),
        transfer_min AS (
            SELECT
                lowerUTF8(toString(token_address)) AS token,
                min(block_time) AS first_time
            FROM evm.transfer_events
            WHERE chain = %(chain)s
              AND lowerUTF8(toString(token_address)) IN (SELECT token FROM tokens)
            GROUP BY token
        ),
        swap_min AS (
            SELECT
                token,
                min(block_time) AS first_time
            FROM (
                SELECT lowerUTF8(toString(base_coin)) AS token, block_time
                FROM evm.swap_events
                WHERE chain = %(chain)s
                  AND lowerUTF8(toString(base_coin)) IN (SELECT token FROM tokens)
                UNION ALL
                SELECT lowerUTF8(toString(quote_coin)) AS token, block_time
                FROM evm.swap_events
                WHERE chain = %(chain)s
                  AND lowerUTF8(toString(quote_coin)) IN (SELECT token FROM tokens)
            )
            GROUP BY token
        )
        SELECT
            token AS mint,
            min(first_time) AS first_tx_date
        FROM (
            SELECT * FROM transfer_min
            UNION ALL
            SELECT * FROM swap_min
        )
        GROUP BY token
        """

        logger.info(f"[{chain}] Computing first_tx_date (full history)")
        rows: List[dict] = self.db_client.execute_query_dict(query, parameters={"chain": chain})
        if not rows:
            return pl.DataFrame({"mint": [], "first_tx_date": []}, schema={"mint": pl.Utf8, "first_tx_date": pl.Datetime})

        out = []
        for r in rows:
            token = r.get("mint")
            if isinstance(token, bytes):
                token = token.decode("utf-8", errors="ignore").rstrip("\x00")
            token = str(token).rstrip("\x00").lower()
            out.append({"mint": token, "first_tx_date": r.get("first_tx_date")})

        return pl.DataFrame(out, schema={"mint": pl.Utf8, "first_tx_date": pl.Datetime})
