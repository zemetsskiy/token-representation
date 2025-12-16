import logging
from datetime import datetime
from typing import List

from ...database import ClickHouseClient
from ..config import EvmChainConfig, EvmConfig

logger = logging.getLogger(__name__)


class EvmTokenDiscovery:
    def __init__(self, db_client: ClickHouseClient, chain_cfg: EvmChainConfig):
        self.db_client = db_client
        self.chain_cfg = chain_cfg

    def discover_tokens(self, window_start: datetime, window_end: datetime, min_swaps: int) -> List[str]:
        chain = self.chain_cfg.chain
        stables = list(self.chain_cfg.stablecoins.values())
        wrapped = self.chain_cfg.wrapped_native.lower() if self.chain_cfg.wrapped_native else ""

        if not stables and not wrapped:
            logger.warning(f"[{chain}] No stablecoins or wrapped native configured; discovery will return empty.")
            return []

        quote_assets = [a.lower() for a in stables]
        if wrapped:
            quote_assets.append(wrapped)
        quote_assets_sql = ", ".join([f"'{a}'" for a in quote_assets])

        excluded = set(quote_assets)
        excluded_sql = ", ".join([f"'{a}'" for a in excluded if a])

        query = f"""
        SELECT
            token,
            count() AS swaps
        FROM (
            SELECT
                if(lowerUTF8(toString(base_coin)) IN ({quote_assets_sql}), lowerUTF8(toString(quote_coin)), lowerUTF8(toString(base_coin))) AS token
            FROM evm.swap_events
            WHERE chain = %(chain)s
              AND block_time >= %(start)s
              AND block_time < %(end)s
              AND (
                  (lowerUTF8(toString(base_coin)) IN ({quote_assets_sql}) AND lowerUTF8(toString(quote_coin)) NOT IN ({quote_assets_sql}))
                  OR
                  (lowerUTF8(toString(quote_coin)) IN ({quote_assets_sql}) AND lowerUTF8(toString(base_coin)) NOT IN ({quote_assets_sql}))
              )
        )
        WHERE token != '' AND token != '0x0000000000000000000000000000000000000000'
          AND token NOT IN ({excluded_sql})
        GROUP BY token
        HAVING swaps >= %(min_swaps)s
        ORDER BY swaps DESC
        """

        logger.info(f"[{chain}] Discovering tokens from evm.swap_events in window (min_swaps={min_swaps}, quote_assets={len(quote_assets)})")
        rows = self.db_client.execute_query_dict(
            query,
            parameters={
                "chain": chain,
                "start": window_start,
                "end": window_end,
                "min_swaps": int(min_swaps or EvmConfig.DEFAULT_MIN_SWAPS),
            },
        )

        tokens: List[str] = []
        for r in rows:
            token_value = r.get("token")
            if isinstance(token_value, bytes):
                token = token_value.decode("utf-8", errors="ignore").rstrip("\x00").lower()
            else:
                token = str(token_value).rstrip("\x00").lower()
            if token:
                tokens.append(token)

        logger.info(f"[{chain}] Discovered {len(tokens):,} tokens")
        return tokens
