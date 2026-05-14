import logging
import time
from typing import Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from ...config import Config

logger = logging.getLogger(__name__)

RPC_BATCH_SIZE = 100
# Short connect timeout: RPC lives in the same datacenter, anything > a few
# seconds is a stuck SYN, not a slow server. Long connect timeouts here used
# to burn the entire 540s job budget on stalled batches.
RPC_CONNECT_TIMEOUT = 3
RPC_READ_TIMEOUT = 15
RPC_RETRY_ATTEMPTS = 3
RPC_RETRY_BACKOFF = 1  # seconds: 1, 2, 4


def _make_session() -> requests.Session:
    """Session with a small keep-alive connection pool, so all batches reuse
    one warm TCP connection to the RPC instead of opening a fresh socket
    per batch (which exhausts ephemeral ports / conntrack at scale)."""
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    return s


class DecimalsResolver:

    def __init__(self):
        self.rpc_url = Config.SOLANA_HTTP_RPC_URL
        if not self.rpc_url:
            raise ValueError('SOLANA_HTTP_RPC_URL is not set in the environment.')
        self.decimals_cache: Dict[str, Optional[int]] = {
            'So11111111111111111111111111111111111111112': 9,
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6,
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6
        }
        self.supply_cache: Dict[str, int] = {}
        self._session = _make_session()

    def resolve_decimals_batch(self, token_addresses: List[str]) -> Dict[str, Optional[int]]:
        if not token_addresses:
            return {}

        logger.info(f'Resolving decimals for {len(token_addresses)} tokens via RPC...')

        # Tokens needing decimals resolution (not cached)
        need_decimals: List[str] = []
        # Tokens needing supply only (decimals cached but no supply yet)
        need_supply: List[str] = []
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            if not s:
                continue
            if s not in self.decimals_cache:
                need_decimals.append(s)
            elif s not in self.supply_cache:
                need_supply.append(s)
        normalized = need_decimals + need_supply
        logger.info(f'  RPC calls needed: {len(need_decimals)} for decimals, {len(need_supply)} for supply only')

        total_batches = (len(normalized) + RPC_BATCH_SIZE - 1) // RPC_BATCH_SIZE
        for i in range(0, len(normalized), RPC_BATCH_SIZE):
            batch = normalized[i:i + RPC_BATCH_SIZE]
            batch_num = i // RPC_BATCH_SIZE + 1
            self._resolve_one_batch(batch, batch_num, total_batches)

        result = {}
        found_count = 0
        not_found_count = 0
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            decimals = self.decimals_cache.get(s, None)
            result[s] = decimals
            if decimals is not None:
                found_count += 1
            else:
                not_found_count += 1

        logger.info(f'Finished resolving decimals. Total cached: {len(self.decimals_cache)}')
        logger.info(f'  Decimals found: {found_count}/{len(token_addresses)} ({100*found_count/len(token_addresses):.1f}%)')
        if not_found_count > 0:
            logger.info(f'  Decimals NOT found: {not_found_count} (token accounts may not exist on-chain)')
        return result

    def _resolve_one_batch(self, batch: List[str], batch_num: int, total_batches: int) -> None:
        """Resolve one batch via a single `getMultipleAccounts` call.

        Solana RPC guarantees positional ordering: response value[i] corresponds
        to batch[i], so we match by index instead of JSON-RPC id.
        """
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getMultipleAccounts',
            'params': [batch, {'encoding': 'jsonParsed'}],
        }

        for attempt in range(RPC_RETRY_ATTEMPTS):
            try:
                resp = self._session.post(
                    self.rpc_url,
                    json=payload,
                    timeout=(RPC_CONNECT_TIMEOUT, RPC_READ_TIMEOUT),
                )
                resp.raise_for_status()
                data = resp.json()

                if 'error' in data:
                    raise ValueError(f"RPC error: {data['error']}")

                values = (data.get('result') or {}).get('value')
                if values is None:
                    raise ValueError(f'Missing result.value in response: {data}')
                if len(values) != len(batch):
                    raise ValueError(
                        f'getMultipleAccounts returned {len(values)} items for batch of {len(batch)}'
                    )

                for mint, account in zip(batch, values):
                    decimals, account_exists, supply = self._parse_account(account)
                    if decimals is not None:
                        self.decimals_cache[mint] = int(decimals)
                        logger.debug(f'Resolved decimals for {mint[:8]}...: {decimals}')
                    elif not account_exists:
                        logger.debug(f'Account does not exist for {mint[:8]}..., decimals=None')
                        self.decimals_cache.setdefault(mint, None)
                    else:
                        logger.warning(f'Could not parse decimals for {mint}, decimals=None')
                        self.decimals_cache.setdefault(mint, None)
                    if supply is not None:
                        self.supply_cache[mint] = supply
                return  # success
            except (requests.exceptions.RequestException, ValueError) as e:
                wait = RPC_RETRY_BACKOFF * (2 ** attempt)
                if attempt < RPC_RETRY_ATTEMPTS - 1:
                    logger.warning(f'RPC batch {batch_num}/{total_batches} failed (attempt {attempt+1}): {e}. Retrying in {wait}s...')
                    time.sleep(wait)
                else:
                    logger.error(f'RPC batch {batch_num}/{total_batches} failed after {RPC_RETRY_ATTEMPTS} attempts: {e}')

        # All retries failed — mark unresolved so we don't loop forever
        for mint in batch:
            self.decimals_cache.setdefault(mint, None)

    def _parse_account(self, account: Optional[dict]) -> Tuple[Optional[int], bool, Optional[int]]:
        """Parse one account-info entry from a getMultipleAccounts response.

        Returns: (decimals, account_exists, supply)
        """
        if account is None:
            return (None, False, None)
        try:
            info = account['data']['parsed']['info']
            decimals = info['decimals']
            supply_str = info.get('supply')
            supply = int(supply_str) if supply_str is not None else None
            return (decimals, True, supply)
        except Exception:
            return (None, True, None)
