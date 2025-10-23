import logging
from typing import Dict, List
import requests
from ..config import Config
logger = logging.getLogger(__name__)

class DecimalsResolver:

    def __init__(self):
        self.rpc_url = Config.SOLANA_HTTP_RPC_URL
        if not self.rpc_url:
            raise ValueError('SOLANA_HTTP_RPC_URL is not set in the environment.')
        self.decimals_cache: Dict[str, int] = {'So11111111111111111111111111111111111111112': 9, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6, 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6}

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
        batch_size = 500
        for i in range(0, len(normalized), batch_size):
            batch = normalized[i:i + batch_size]

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

            try:
                resp = requests.post(self.rpc_url, json=payload, timeout=30)
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
                    decimals = self._parse_rpc_response(item)
                    if decimals is not None:
                        self.decimals_cache[mint] = int(decimals)
                        logger.debug(f'Resolved decimals for {mint[:8]}...: {decimals}')
                    else:
                        logger.warning(f'Could not resolve decimals for {mint}, defaulting to 6')
                        self.decimals_cache.setdefault(mint, 6)
            except requests.exceptions.RequestException as e:
                logger.error(f'RPC request failed: {e}')
                for mint in batch:
                    self.decimals_cache.setdefault(mint, 6)
        result = {}
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            result[s] = self.decimals_cache.get(s, 6)
        logger.info(f'Finished resolving decimals. Total cached: {len(self.decimals_cache)}')
        return result

    def _parse_rpc_response(self, item: dict) -> int | None:
        try:
            return item['result']['value']['data']['parsed']['info']['decimals']
        except Exception:
            return None