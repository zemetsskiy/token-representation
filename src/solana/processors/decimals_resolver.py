import logging
import time
from typing import Dict, List
import requests
from ...config import Config

logger = logging.getLogger(__name__)

RPC_BATCH_SIZE = 100
RPC_TIMEOUT = 30
RPC_RETRY_ATTEMPTS = 3
RPC_RETRY_BACKOFF = 2
RPC_BATCH_DELAY = 0.1


class DecimalsResolver:

    def __init__(self):
        self.rpc_url = Config.SOLANA_HTTP_RPC_URL
        if not self.rpc_url:
            raise ValueError('SOLANA_HTTP_RPC_URL is not set in the environment.')
        self.decimals_cache: Dict[str, int] = {
            'So11111111111111111111111111111111111111112': 9,
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6,
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6
        }

    def resolve_decimals_batch(self, token_addresses: List[str]) -> Dict[str, int]:
        if not token_addresses:
            return {}

        logger.info(f'Resolving decimals for {len(token_addresses)} tokens via RPC...')

        normalized: List[str] = []
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            if s and s not in self.decimals_cache:
                normalized.append(s)
        total_batches = (len(normalized) + RPC_BATCH_SIZE - 1) // RPC_BATCH_SIZE
        for i in range(0, len(normalized), RPC_BATCH_SIZE):
            batch = normalized[i:i + RPC_BATCH_SIZE]
            batch_num = i // RPC_BATCH_SIZE + 1

            # Create mapping from id to mint address
            id_to_mint = {}
            payload = []
            for idx, mint in enumerate(batch):
                request_id = idx + 1
                id_to_mint[request_id] = mint
                payload.append({
                    'jsonrpc': '2.0',
                    'id': request_id,
                    'method': 'getAccountInfo',
                    'params': [mint, {'encoding': 'jsonParsed'}]
                })

            success = False
            for attempt in range(RPC_RETRY_ATTEMPTS):
                try:
                    resp = requests.post(self.rpc_url, json=payload, timeout=RPC_TIMEOUT)
                    resp.raise_for_status()
                    results = resp.json()

                    # Handle single response wrapped in dict
                    if isinstance(results, dict) and 'result' in results:
                        results = [results]

                    # Match responses by 'id' field (responses may be out of order)
                    for item in results:
                        response_id = item.get('id')
                        if response_id is None or response_id not in id_to_mint:
                            logger.debug(f'Received response with unexpected id: {response_id}')
                            continue

                        mint = id_to_mint[response_id]
                        decimals, account_exists = self._parse_rpc_response(item)

                        if decimals is not None:
                            self.decimals_cache[mint] = int(decimals)
                            logger.debug(f'Resolved decimals for {mint[:8]}...: {decimals}')
                        elif not account_exists:
                            logger.debug(f'Account does not exist for {mint[:8]}..., decimals=None')
                            self.decimals_cache.setdefault(mint, None)
                        else:
                            logger.warning(f'Could not parse decimals for {mint}, decimals=None')
                            self.decimals_cache.setdefault(mint, None)
                    success = True
                    break
                except requests.exceptions.RequestException as e:
                    wait = RPC_RETRY_BACKOFF ** attempt
                    if attempt < RPC_RETRY_ATTEMPTS - 1:
                        logger.warning(f'RPC batch {batch_num}/{total_batches} failed (attempt {attempt+1}): {e}. Retrying in {wait}s...')
                        time.sleep(wait)
                    else:
                        logger.error(f'RPC batch {batch_num}/{total_batches} failed after {RPC_RETRY_ATTEMPTS} attempts: {e}')

            if not success:
                for mint in batch:
                    self.decimals_cache.setdefault(mint, None)

            if RPC_BATCH_DELAY > 0 and i + RPC_BATCH_SIZE < len(normalized):
                time.sleep(RPC_BATCH_DELAY)
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

    def _parse_rpc_response(self, item: dict) -> tuple[int | None, bool]:
        """
        Parse RPC response for decimals.

        Returns:
            Tuple of (decimals, account_exists)
        """
        try:
            result = item.get('result', {})
            value = result.get('value')

            # Account doesn't exist on chain
            if value is None:
                return (None, False)

            decimals = value['data']['parsed']['info']['decimals']
            return (decimals, True)
        except Exception:
            return (None, True)  # Exists but failed to parse