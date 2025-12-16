import logging
from datetime import datetime
from typing import List, Optional

import polars as pl

from ...config import Config
from ...database import ClickHouseClient, get_redis_client
from ...database.redis_client import RedisPriceNotFoundError
from ..config import EvmChainConfig

logger = logging.getLogger(__name__)


class EvmPriceCalculator:
    def __init__(self, db_client: ClickHouseClient, chain_cfg: EvmChainConfig):
        self.db_client = db_client
        self.chain_cfg = chain_cfg
        self._redis = None
        self._native_price_usd: Optional[float] = None

    def _get_native_price_usd(self) -> Optional[float]:
        if self._native_price_usd is not None:
            return self._native_price_usd

        chain = self.chain_cfg.chain
        try:
            if self._redis is None:
                self._redis = get_redis_client()
            self._native_price_usd = self._redis.get_native_price(chain)
            return self._native_price_usd
        except RedisPriceNotFoundError as e:
            logger.warning(f"[{chain}] Could not get native price from Redis: {e}")
            return None
        except Exception as e:
            logger.warning(f"[{chain}] Unexpected error fetching native price: {e}")
            return None

    def get_prices_for_chunk(self, window_start: datetime, window_end: datetime) -> pl.DataFrame:
        temp_db = Config.CLICKHOUSE_TEMP_DATABASE
        chain = self.chain_cfg.chain
        stables = list(self.chain_cfg.stablecoins.values())
        wrapped = (self.chain_cfg.wrapped_native or "").lower()
        if not stables and not wrapped:
            return pl.DataFrame(
                {"mint": [], "price_usd": [], "price_method": []},
                schema={"mint": pl.Utf8, "price_usd": pl.Float64, "price_method": pl.Utf8},
            )

        native_price_usd = self._get_native_price_usd()
        if native_price_usd:
            logger.info(f"[{chain}] Using native price from Redis: ${native_price_usd:.2f}")
        else:
            logger.warning(f"[{chain}] Native price not available in Redis, NATIVE_VWAP pricing disabled")

        stables_sql = ", ".join([f"'{a.lower()}'" for a in stables])
        wrapped_sql = f"'{wrapped}'" if wrapped else "''"

        query = f"""
        WITH
        tokens AS (SELECT mint FROM {temp_db}.chunk_tokens),
        token_vs_stable AS (
            SELECT
                token,
                sum(stable_amount_norm) / greatest(sum(token_amount_norm), 1e-18) AS stable_price_usd
            FROM (
                SELECT
                    if(lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens), lowerUTF8(toString(base_coin)), lowerUTF8(toString(quote_coin))) AS token,
                    (if(lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens), base_coin_amount, quote_coin_amount) / pow(10,
                        if(lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens), base_coin_decimals, quote_coin_decimals)
                    )) AS token_amount_norm,
                    (if(lowerUTF8(toString(base_coin)) IN ({stables_sql}), base_coin_amount, quote_coin_amount) / pow(10,
                        if(lowerUTF8(toString(base_coin)) IN ({stables_sql}), base_coin_decimals, quote_coin_decimals)
                    )) AS stable_amount_norm
                FROM evm.swap_events
                WHERE chain = %(chain)s
                  AND block_time >= %(start)s
                  AND block_time < %(end)s
                  AND (
                    (lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens) AND lowerUTF8(toString(quote_coin)) IN ({stables_sql}))
                    OR
                    (lowerUTF8(toString(quote_coin)) IN (SELECT mint FROM tokens) AND lowerUTF8(toString(base_coin)) IN ({stables_sql}))
                  )
                  AND base_coin_amount > 0
                  AND quote_coin_amount > 0
            )
            GROUP BY token
        ),
        token_vs_native AS (
            SELECT
                token,
                sum(native_amount_norm) / greatest(sum(token_amount_norm), 1e-18) AS native_price
            FROM (
                SELECT
                    if(lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens), lowerUTF8(toString(base_coin)), lowerUTF8(toString(quote_coin))) AS token,
                    (if(lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens), base_coin_amount, quote_coin_amount) / pow(10,
                        if(lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens), base_coin_decimals, quote_coin_decimals)
                    )) AS token_amount_norm,
                    (if(lowerUTF8(toString(base_coin)) = {wrapped_sql}, base_coin_amount, quote_coin_amount) / pow(10,
                        if(lowerUTF8(toString(base_coin)) = {wrapped_sql}, base_coin_decimals, quote_coin_decimals)
                    )) AS native_amount_norm
                FROM evm.swap_events
                WHERE chain = %(chain)s
                  AND block_time >= %(start)s
                  AND block_time < %(end)s
                  AND {wrapped_sql} != ''
                  AND (
                    (lowerUTF8(toString(base_coin)) IN (SELECT mint FROM tokens) AND lowerUTF8(toString(quote_coin)) = {wrapped_sql})
                    OR
                    (lowerUTF8(toString(quote_coin)) IN (SELECT mint FROM tokens) AND lowerUTF8(toString(base_coin)) = {wrapped_sql})
                  )
                  AND base_coin_amount > 0
                  AND quote_coin_amount > 0
            )
            GROUP BY token
        )
        SELECT
            t.mint,
            s.stable_price_usd,
            n.native_price
        FROM tokens t
        LEFT JOIN token_vs_stable s ON s.token = t.mint
        LEFT JOIN token_vs_native n ON n.token = t.mint
        """

        logger.info(f"[{chain}] Computing VWAP prices vs stablecoins (window)")
        rows: List[dict] = self.db_client.execute_query_dict(
            query,
            parameters={"chain": chain, "start": window_start, "end": window_end},
        )
        if not rows:
            return pl.DataFrame(
                {"mint": [], "price_usd": [], "price_method": []},
                schema={"mint": pl.Utf8, "price_usd": pl.Float64, "price_method": pl.Utf8},
            )

        out = []
        for r in rows:
            token = r.get("mint")
            if isinstance(token, bytes):
                token = token.decode("utf-8", errors="ignore").rstrip("\x00")
            token = str(token).rstrip("\x00").lower()

            stable_price = float(r.get("stable_price_usd", 0) or 0)
            native_price = float(r.get("native_price", 0) or 0)

            if stable_price > 0:
                price_usd = stable_price
                price_method = "STABLE_VWAP"
            elif native_price > 0 and native_price_usd and native_price_usd > 0:
                price_usd = native_price * native_price_usd
                price_method = "NATIVE_VWAP"
            else:
                price_usd = 0.0
                price_method = "NONE"

            out.append({"mint": token, "price_usd": price_usd, "price_method": price_method})

        return pl.DataFrame(out, schema={"mint": pl.Utf8, "price_usd": pl.Float64, "price_method": pl.Utf8})
