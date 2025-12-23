import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from ..config import EvmConfig

logger = logging.getLogger(__name__)


class EvmRpcError(Exception):
    pass


def _chunks(seq: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _hex_to_int(hex_str: str) -> int:
    if not hex_str or hex_str == "0x":
        return 0
    return int(hex_str, 16)


def _decode_erc20_uint256(result_hex: str) -> Optional[int]:
    if not result_hex or result_hex == "0x":
        return None
    try:
        return _hex_to_int(result_hex)
    except Exception:
        return None


def _decode_erc20_uint8(result_hex: str) -> Optional[int]:
    v = _decode_erc20_uint256(result_hex)
    if v is None:
        return None
    if v < 0 or v > 255:
        return None
    return int(v)


def _decode_erc20_string(result_hex: str) -> Optional[str]:
    if not result_hex or result_hex == "0x":
        return None
    raw = bytes.fromhex(result_hex[2:])
    if len(raw) == 32:
        try:
            return raw.rstrip(b"\x00").decode("utf-8", errors="replace").strip() or None
        except Exception:
            return None
    if len(raw) < 64:
        return None
    try:
        offset = int.from_bytes(raw[0:32], "big")
        if offset + 32 > len(raw):
            return None
        strlen = int.from_bytes(raw[offset : offset + 32], "big")
        start = offset + 32
        end = start + strlen
        if end > len(raw):
            return None
        return raw[start:end].decode("utf-8", errors="replace").strip() or None
    except Exception:
        return None


class EvmRpcClient:
    _DECIMALS = "0x313ce567"
    _SYMBOL = "0x95d89b41"
    _NAME = "0x06fdde03"
    _TOTAL_SUPPLY = "0x18160ddd"

    def __init__(self, chain: str, rpc_url: str):
        self.chain = chain
        self.rpc_url = rpc_url
        self._session = requests.Session()
        self._lock = threading.Lock()

    def _post(self, payload: Any) -> Any:
        try:
            resp = self._session.post(
                self.rpc_url,
                json=payload,
                timeout=EvmConfig.RPC_TIMEOUT_SECONDS,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise EvmRpcError(f"{self.chain} RPC request failed: {e}") from e

    def eth_call_batch(self, calls: List[Tuple[str, str]]) -> Dict[Tuple[str, str], Optional[str]]:
        if not calls:
            return {}

        results: Dict[Tuple[str, str], Optional[str]] = {}
        for batch in _chunks(calls, EvmConfig.RPC_MAX_BATCH):
            payload = []
            for idx, (to, data) in enumerate(batch):
                payload.append(
                    {
                        "jsonrpc": "2.0",
                        "id": idx,
                        "method": "eth_call",
                        "params": [{"to": to, "data": data}, "latest"],
                    }
                )

            response = self._post(payload)
            id_to_item = {item.get("id"): item for item in (response or []) if isinstance(item, dict)}
            for idx, (to, data) in enumerate(batch):
                item = id_to_item.get(idx) or {}
                if "error" in item:
                    results[(to, data)] = None
                else:
                    results[(to, data)] = item.get("result")

        return results

    def _call_erc20_single(self, token: str, selector: str) -> Optional[str]:
        res = self.eth_call_batch([(token, selector)])
        return res.get((token, selector))

    def get_token_metadata_batch(self, tokens: List[str]) -> Dict[str, Dict[str, Any]]:
        tokens_lc = [t.lower() for t in tokens]
        calls: List[Tuple[str, str]] = []
        for t in tokens_lc:
            calls.append((t, self._DECIMALS))
            calls.append((t, self._SYMBOL))
            calls.append((t, self._NAME))

        raw = self.eth_call_batch(calls)
        out: Dict[str, Dict[str, Any]] = {t: {"decimals": None, "symbol": None, "name": None} for t in tokens_lc}
        for t in tokens_lc:
            out[t]["decimals"] = _decode_erc20_uint8(raw.get((t, self._DECIMALS)))
            out[t]["symbol"] = _decode_erc20_string(raw.get((t, self._SYMBOL)))
            out[t]["name"] = _decode_erc20_string(raw.get((t, self._NAME)))
        return out

    def get_total_supply_batch(self, tokens: List[str]) -> Dict[str, Optional[int]]:
        tokens_lc = [t.lower() for t in tokens]
        calls = [(t, self._TOTAL_SUPPLY) for t in tokens_lc]
        raw = self.eth_call_batch(calls)
        return {t: _decode_erc20_uint256(raw.get((t, self._TOTAL_SUPPLY))) for t in tokens_lc}

    def enrich_tokens_parallel(
        self, tokens: List[str]
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Optional[int]]]:
        tokens_lc = [t.lower() for t in tokens]
        if not tokens_lc:
            return {}, {}

        meta: Dict[str, Dict[str, Any]] = {}
        supply: Dict[str, Optional[int]] = {}

        def meta_job(batch: List[str]) -> Dict[str, Dict[str, Any]]:
            return self.get_token_metadata_batch(batch)

        def supply_job(batch: List[str]) -> Dict[str, Optional[int]]:
            return self.get_total_supply_batch(batch)

        batch_size = max(10, min(EvmConfig.RPC_MAX_BATCH // 3, 100))
        meta_batches = list(_chunks(tokens_lc, batch_size))
        supply_batches = list(_chunks(tokens_lc, EvmConfig.RPC_MAX_BATCH))

        with ThreadPoolExecutor(max_workers=EvmConfig.RPC_MAX_WORKERS) as ex:
            futures = []
            for b in meta_batches:
                futures.append(ex.submit(meta_job, b))
            for b in supply_batches:
                futures.append(ex.submit(supply_job, b))

            for fut in as_completed(futures):
                res = fut.result()
                any_val = next(iter(res.values())) if res else None
                if isinstance(any_val, dict):
                    meta.update(res)  # type: ignore[arg-type]
                else:
                    supply.update(res)  # type: ignore[arg-type]

        return meta, supply

