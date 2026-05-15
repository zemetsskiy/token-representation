import logging
from typing import Any, Dict, List, Optional

from ..config import EvmChainConfig
from ..rpc import EvmRpcClient

logger = logging.getLogger(__name__)


class EvmRpcEnricher:
    def __init__(self, chain_cfg: EvmChainConfig):
        self.chain_cfg = chain_cfg
        self.rpc = EvmRpcClient(chain=chain_cfg.chain, rpc_url=chain_cfg.rpc_url)

        self._meta_cache: Dict[str, Dict[str, Any]] = {}
        self._supply_cache: Dict[str, Optional[int]] = {}

    def enrich(self, tokens: List[str]) -> List[Dict[str, Any]]:
        tokens_lc = [t.lower() for t in tokens]
        missing = [t for t in tokens_lc if t not in self._meta_cache or t not in self._supply_cache]
        if missing:
            logger.info(f"[{self.chain_cfg.chain}] RPC enriching {len(missing):,}/{len(tokens_lc):,} tokens")
            meta, supply = self.rpc.enrich_tokens_parallel(missing)
            self._meta_cache.update(meta)
            self._supply_cache.update(supply)

        out: List[Dict[str, Any]] = []
        for t in tokens_lc:
            m = self._meta_cache.get(t, {})
            supply_int = self._supply_cache.get(t)
            # Cast uint256 supply to float here. Some ERC20s (scam/meme) have
            # totalSupply > 2^63, which overflows polars Int64 dtype inference
            # at pl.DataFrame(out) construction. Downstream math divides this
            # by 10^decimals as Float64 anyway, so we lose nothing.
            out.append({
                "mint": t,
                "decimals_rpc": m.get("decimals"),
                "symbol_rpc": m.get("symbol"),
                "name_rpc": m.get("name"),
                "total_supply_raw": float(supply_int) if supply_int is not None else None,
            })
        return out





