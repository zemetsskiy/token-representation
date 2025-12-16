import json
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional


def _env_json(name: str) -> Optional[dict]:
    value = os.getenv(name)
    if not value:
        return None
    return json.loads(value)


@dataclass(frozen=True)
class EvmChainConfig:
    chain: str
    rpc_url: str
    stablecoins: Dict[str, str]
    wrapped_native: str


class EvmConfig:
    DEFAULT_WINDOW_HOURS = int(os.getenv("EVM_WINDOW_HOURS", "24"))
    DEFAULT_MIN_SWAPS = int(os.getenv("EVM_MIN_SWAPS", "25"))
    DEFAULT_CHUNK_SIZE = int(os.getenv("EVM_CHUNK_SIZE", "200000"))

    RPC_TIMEOUT_SECONDS = float(os.getenv("EVM_RPC_TIMEOUT_SECONDS", "10"))
    RPC_MAX_BATCH = int(os.getenv("EVM_RPC_MAX_BATCH", "100"))
    RPC_MAX_WORKERS = int(os.getenv("EVM_RPC_MAX_WORKERS", "16"))

    _CHAIN_CONFIGS_JSON = _env_json("EVM_CHAIN_CONFIGS")
    _RPC_URLS_JSON = _env_json("EVM_RPC_URLS")

    @classmethod
    def _rpc_url_for_chain(cls, chain: str) -> Optional[str]:
        if cls._RPC_URLS_JSON and chain in cls._RPC_URLS_JSON:
            return cls._RPC_URLS_JSON.get(chain)
        return os.getenv(f"EVM_RPC_URL_{chain.upper()}") or os.getenv(f"EVM_RPC_URL_{chain}")

    @classmethod
    def default_chain_configs(cls) -> Dict[str, EvmChainConfig]:
        defaults = {
            "eth": {
                "wrapped_native": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "stablecoins": {
                    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                    "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                },
            },
            "base": {
                "wrapped_native": "0x4200000000000000000000000000000000000006",
                "stablecoins": {
                    "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                },
            },
            "bsc": {
                "wrapped_native": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
                "stablecoins": {
                    "USDT": "0x55d398326f99059fF775485246999027B3197955",
                    "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
                    "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
                },
            },
            "polygon": {
                "wrapped_native": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
                "stablecoins": {
                    "USDC": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                    "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
                    "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
                },
            },
        }

        cfgs: Dict[str, dict] = cls._CHAIN_CONFIGS_JSON or {}
        merged = {**defaults, **cfgs}

        out: Dict[str, EvmChainConfig] = {}
        for chain, spec in merged.items():
            rpc_url = (spec or {}).get("rpc_url") or cls._rpc_url_for_chain(chain)
            if not rpc_url:
                continue
            wrapped_native = (spec or {}).get("wrapped_native") or ""
            stables = (spec or {}).get("stablecoins") or {}
            out[chain] = EvmChainConfig(
                chain=chain,
                rpc_url=rpc_url,
                stablecoins={k: v.lower() for k, v in stables.items()},
                wrapped_native=wrapped_native.lower(),
            )
        return out

    @classmethod
    def window_timedelta(cls, window_hours: Optional[int] = None) -> timedelta:
        return timedelta(hours=int(window_hours or cls.DEFAULT_WINDOW_HOURS))

    @classmethod
    def chains_in_clickhouse(cls, db_client, window_start, window_end) -> List[str]:
        try:
            rows = db_client.execute_query_dict(
                """
                SELECT DISTINCT chain
                FROM evm.swap_events
                WHERE block_time >= %(start)s AND block_time < %(end)s
                ORDER BY chain
                """,
                parameters={"start": window_start, "end": window_end},
            )
        except Exception:
            rows = db_client.execute_query_dict("SELECT DISTINCT chain FROM evm.swap_events ORDER BY chain")

        chains: List[str] = []
        for r in rows:
            v = r.get("chain")
            if isinstance(v, bytes):
                v = v.decode("utf-8", errors="ignore").rstrip("\x00")
            v = str(v).rstrip("\x00")
            if v:
                chains.append(v)
        return chains
