import logging
import time
import base58
import hashlib
import struct
import base64
from typing import Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from ...config import Config

logger = logging.getLogger(__name__)

METAPLEX_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

RPC_BATCH_SIZE = 100
RPC_CONNECT_TIMEOUT = 3
RPC_READ_TIMEOUT = 15
RPC_RETRY_ATTEMPTS = 3
RPC_RETRY_BACKOFF = 1
RPC_BATCH_DELAY = 0.0


def _make_session() -> requests.Session:
    """Session with keep-alive pooling so all batches share one warm TCP
    connection instead of opening a new socket per batch."""
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    return s


class MetadataFetcher:
    def __init__(self):
        self.rpc_url = Config.SOLANA_HTTP_RPC_URL
        if not self.rpc_url:
            raise ValueError('SOLANA_HTTP_RPC_URL is not set in the environment.')
        self.metadata_cache: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}
        self._session = _make_session()

    def resolve_metadata_batch(self, token_addresses: List[str]) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
        """
        Resolve metadata (symbol, name, uri) for a batch of token addresses.
        Uses two strategies:
          1. Metaplex Token Metadata PDA (works for SPL Token mints)
          2. Token-2022 on-chain metadata extension (fallback for Token-2022 mints)

        Args:
            token_addresses: List of token mint addresses

        Returns:
            Dict mapping token address to (symbol, name, uri) tuple
        """
        if not token_addresses:
            return {}

        logger.info(f'Resolving metadata for {len(token_addresses)} tokens...')

        normalized: List[str] = []
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            if s and s not in self.metadata_cache:
                normalized.append(s)

        # Pass 1: Token-2022 jsonParsed (cheap — reads mint account directly, covers pump.fun etc)
        total_batches = (len(normalized) + RPC_BATCH_SIZE - 1) // RPC_BATCH_SIZE
        logger.info(f'Pass 1: Token-2022 jsonParsed for {len(normalized)} tokens ({total_batches} batches)...')
        for i in range(0, len(normalized), RPC_BATCH_SIZE):
            batch = normalized[i:i + RPC_BATCH_SIZE]
            batch_num = i // RPC_BATCH_SIZE + 1
            if batch_num % 50 == 0:
                logger.info(f'Token-2022 progress: batch {batch_num}/{total_batches}')
            self._fetch_token2022_metadata_batch(batch)
            if RPC_BATCH_DELAY > 0 and i + RPC_BATCH_SIZE < len(normalized):
                time.sleep(RPC_BATCH_DELAY)

        # Pass 2: Metaplex PDA for tokens still missing (SPL Token with Metaplex metadata)
        missing = [addr for addr in normalized
                   if self.metadata_cache.get(addr, (None, None, None))[0] is None]

        if missing:
            metaplex_batches = (len(missing) + RPC_BATCH_SIZE - 1) // RPC_BATCH_SIZE
            logger.info(f'Pass 2: Metaplex PDA for {len(missing)} remaining tokens ({metaplex_batches} batches)...')
            for i in range(0, len(missing), RPC_BATCH_SIZE):
                batch = missing[i:i + RPC_BATCH_SIZE]
                batch_num = i // RPC_BATCH_SIZE + 1
                if batch_num % 50 == 0:
                    logger.info(f'Metaplex progress: batch {batch_num}/{metaplex_batches}')
                self._fetch_metadata_batch(batch)
                if RPC_BATCH_DELAY > 0 and i + RPC_BATCH_SIZE < len(missing):
                    time.sleep(RPC_BATCH_DELAY)

        result = {}
        metadata_found = 0
        metadata_not_found = 0
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            metadata = self.metadata_cache.get(s, (None, None, None))
            result[s] = metadata
            if metadata and metadata[0] is not None:
                metadata_found += 1
            else:
                metadata_not_found += 1

        pct_found = 100 * metadata_found / len(token_addresses) if token_addresses else 0
        logger.info(f'Metadata resolved: {metadata_found}/{len(token_addresses)} ({pct_found:.1f}%)')
        if metadata_not_found > 0:
            logger.info(f'  Still missing: {metadata_not_found} tokens')
        return result

    def _fetch_metadata_batch(self, mint_addresses: List[str]):
        """Fetch Metaplex metadata for a batch via getMultipleAccounts on PDAs."""
        metadata_accounts: List[Tuple[str, str]] = []
        for mint in mint_addresses:
            metadata_pda = self._derive_metadata_pda(mint)
            if metadata_pda:
                metadata_accounts.append((mint, metadata_pda))
            else:
                logger.debug(f'Could not derive metadata PDA for {mint}')
                self.metadata_cache[mint] = (None, None, None)

        if not metadata_accounts:
            return

        pdas = [pda for _, pda in metadata_accounts]
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getMultipleAccounts',
            'params': [pdas, {'encoding': 'base64'}],
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
                if len(values) != len(metadata_accounts):
                    raise ValueError(
                        f'getMultipleAccounts returned {len(values)} items for {len(metadata_accounts)} PDAs'
                    )

                found_count = 0
                for (mint, metadata_pda), account in zip(metadata_accounts, values):
                    metadata = self._parse_metadata_account_value(account)
                    self.metadata_cache[mint] = metadata
                    if metadata and metadata[0]:
                        found_count += 1
                        logger.debug(f'Found metadata for {mint[:8]}...: symbol={metadata[0]}, name={metadata[1]}')

                if found_count > 0:
                    logger.debug(f'Metaplex batch: {found_count}/{len(metadata_accounts)} resolved')
                return

            except (requests.exceptions.RequestException, ValueError) as e:
                wait = RPC_RETRY_BACKOFF * (2 ** attempt)
                if attempt < RPC_RETRY_ATTEMPTS - 1:
                    logger.warning(f'Metaplex metadata batch failed (attempt {attempt+1}): {e}. Retrying in {wait}s...')
                    time.sleep(wait)
                else:
                    logger.error(f'Metaplex metadata batch failed after {RPC_RETRY_ATTEMPTS} attempts: {e}')
            except Exception as e:
                logger.error(f'Unexpected error processing metadata batch: {e}', exc_info=True)
                break

        for mint, _ in metadata_accounts:
            self.metadata_cache.setdefault(mint, (None, None, None))

    def _fetch_token2022_metadata_batch(self, mint_addresses: List[str]):
        """Fetch Token-2022 on-chain metadata for a batch via getMultipleAccounts."""
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getMultipleAccounts',
            'params': [mint_addresses, {'encoding': 'jsonParsed'}],
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
                if len(values) != len(mint_addresses):
                    raise ValueError(
                        f'getMultipleAccounts returned {len(values)} items for {len(mint_addresses)} mints'
                    )

                found_count = 0
                for mint, account in zip(mint_addresses, values):
                    metadata = self._parse_token2022_metadata_value(account)
                    if metadata[0] is not None:
                        self.metadata_cache[mint] = metadata
                        found_count += 1

                if found_count > 0:
                    logger.debug(f'Token-2022 batch: found {found_count}/{len(mint_addresses)}')
                return

            except (requests.exceptions.RequestException, ValueError) as e:
                wait = RPC_RETRY_BACKOFF * (2 ** attempt)
                if attempt < RPC_RETRY_ATTEMPTS - 1:
                    logger.warning(f'Token-2022 metadata batch failed (attempt {attempt+1}): {e}. Retrying in {wait}s...')
                    time.sleep(wait)
                else:
                    logger.error(f'Token-2022 metadata batch failed after {RPC_RETRY_ATTEMPTS} attempts: {e}')
            except Exception as e:
                logger.error(f'Unexpected error in Token-2022 metadata batch: {e}', exc_info=True)
                break

    def _parse_token2022_metadata_value(self, value: Optional[dict]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Same as `_parse_token2022_metadata` but takes the account-info `value`
        object directly (as returned by getMultipleAccounts in result.value[i]).
        """
        return self._parse_token2022_metadata({'result': {'value': value}})

    def _parse_metadata_account_value(self, value: Optional[dict]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Adapter: `_parse_metadata_account` expects {'result': {'value': ...}}
        but getMultipleAccounts gives us the value object directly."""
        return self._parse_metadata_account({'result': {'value': value}})

    def _parse_token2022_metadata(self, rpc_response: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse tokenMetadata extension from jsonParsed getAccountInfo response."""
        try:
            value = rpc_response.get('result', {}).get('value')
            if not value:
                return (None, None, None)

            data = value.get('data', {})
            if not isinstance(data, dict):
                return (None, None, None)

            parsed = data.get('parsed', {})
            info = parsed.get('info', {})
            extensions = info.get('extensions', [])

            for ext in extensions:
                if ext.get('extension') == 'tokenMetadata':
                    state = ext.get('state', {})
                    symbol = state.get('symbol', '').strip() or None
                    name = state.get('name', '').strip() or None
                    uri = state.get('uri', '').strip() or None
                    return (symbol, name, uri)

            return (None, None, None)
        except Exception as e:
            logger.debug(f'Failed to parse Token-2022 metadata: {e}')
            return (None, None, None)

    def _derive_metadata_pda(self, mint_address: str) -> Optional[str]:
        """
        Derive the Metaplex metadata PDA for a given mint address.

        Args:
            mint_address: Token mint address

        Returns:
            Metadata PDA address or None if derivation fails
        """
        try:
            # Decode addresses from base58
            program_id_bytes = base58.b58decode(METAPLEX_PROGRAM_ID)
            mint_bytes = base58.b58decode(mint_address)

            # Seeds for PDA derivation
            seeds = [
                b"metadata",
                program_id_bytes,
                mint_bytes
            ]

            # Find program address
            pda, _ = self._find_program_address(seeds, program_id_bytes)
            return base58.b58encode(pda).decode('utf-8')

        except Exception as e:
            logger.debug(f'Failed to derive metadata PDA for {mint_address}: {e}')
            return None

    def _find_program_address(self, seeds: List[bytes], program_id: bytes) -> Tuple[bytes, int]:
        """
        Find a valid program derived address and its bump seed.

        Args:
            seeds: List of seed bytes
            program_id: Program ID bytes

        Returns:
            Tuple of (PDA bytes, bump seed)
        """
        for bump in range(256, 0, -1):
            try:
                seeds_with_bump = seeds + [bytes([bump - 1])]
                pda = self._create_program_address(seeds_with_bump, program_id)
                return pda, bump - 1
            except ValueError:
                continue
        raise ValueError("Unable to find a viable program address bump seed")

    def _create_program_address(self, seeds: List[bytes], program_id: bytes) -> bytes:
        """
        Create a program address (PDA).

        Args:
            seeds: List of seed bytes
            program_id: Program ID bytes

        Returns:
            PDA bytes

        Raises:
            ValueError: If the derived address is on the ed25519 curve
        """
        hasher = hashlib.sha256()
        for seed in seeds:
            hasher.update(seed)
        hasher.update(program_id)
        hasher.update(b"ProgramDerivedAddress")

        pda = hasher.digest()

        if self._is_on_curve(pda):
            raise ValueError("Address is on curve")

        return pda

    def _is_on_curve(self, pubkey: bytes) -> bool:
        """
        Check if a 32-byte public key is a valid ed25519 curve point.
        PDAs must NOT be on the curve — this rejects valid curve points
        so the PDA derivation tries the next bump seed.
        """
        p = (1 << 255) - 19

        # Decode y-coordinate (little-endian, clear sign bit)
        y = int.from_bytes(pubkey, 'little') & ((1 << 255) - 1)

        if y >= p:
            return False

        # ed25519 curve constant: d = -121665/121666 mod p
        d = (-121665 * pow(121666, p - 2, p)) % p

        # From -x^2 + y^2 = 1 + d*x^2*y^2, solve for x^2:
        # x^2 = (y^2 - 1) / (d*y^2 + 1) mod p
        y2 = (y * y) % p
        num = (y2 - 1) % p
        den = (d * y2 + 1) % p

        if den == 0:
            return num == 0

        x2 = (num * pow(den, p - 2, p)) % p

        if x2 == 0:
            return True

        # Euler's criterion: x2 is a quadratic residue iff x2^((p-1)/2) ≡ 1 (mod p)
        return pow(x2, (p - 1) // 2, p) == 1

    def _parse_metadata_account(self, rpc_response: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Parse Metaplex metadata account data according to the Metaplex Token Metadata standard.

        Args:
            rpc_response: RPC response containing account data

        Returns:
            Tuple of (symbol, name, uri)
        """
        try:
            result = rpc_response.get('result')
            if not result:
                logger.debug('No result in RPC response')
                return (None, None, None)

            value = result.get('value')
            if not value:
                logger.debug('No value in result (account does not exist)')
                return (None, None, None)

            account_data = value.get('data')
            if not account_data or not isinstance(account_data, list) or len(account_data) < 1:
                logger.debug(f'Invalid account data format: {type(account_data)}')
                return (None, None, None)

            # Decode base64 data
            try:
                data_bytes = base64.b64decode(account_data[0])
            except Exception as e:
                logger.debug(f'Failed to decode base64 data: {e}')
                return (None, None, None)

            logger.debug(f'Decoded {len(data_bytes)} bytes of metadata')

            # Metaplex metadata structure (fixed-size fields):
            # - key (1 byte)
            # - update_authority (32 bytes)
            # - mint (32 bytes)
            # - name (4 bytes length + 32 bytes fixed data)
            # - symbol (4 bytes length + 10 bytes fixed data)
            # - uri (4 bytes length + 200 bytes fixed data)

            if len(data_bytes) < 65:
                logger.debug(f'Data too short: {len(data_bytes)} bytes')
                return (None, None, None)

            offset = 65  # Skip key (1) + update_authority (32) + mint (32)

            # Read name (4-byte length prefix + 32-byte fixed size)
            name = self._read_string(data_bytes, offset)
            offset += 4 + 32  # Always increment by fixed size

            # Read symbol (4-byte length prefix + 10-byte fixed size)
            symbol = self._read_string(data_bytes, offset)
            offset += 4 + 10  # Always increment by fixed size

            # Read URI (4-byte length prefix + 200-byte fixed size)
            uri = self._read_string(data_bytes, offset)

            if symbol or name or uri:
                logger.debug(f'Parsed metadata: symbol="{symbol}", name="{name}", uri="{uri}"')

            return (symbol, name, uri)

        except Exception as e:
            logger.debug(f'Failed to parse metadata account: {e}', exc_info=True)
            return (None, None, None)

    def _read_string(self, data: bytes, offset: int) -> Optional[str]:
        """
        Read a Rust String from bytes (4-byte little-endian length + UTF-8 data).

        Args:
            data: Byte array
            offset: Starting offset

        Returns:
            Decoded string or None
        """
        try:
            if offset + 4 > len(data):
                return None

            # Read 4-byte little-endian length prefix
            length = struct.unpack('<I', data[offset:offset + 4])[0]

            if length == 0:
                return None  # Empty string

            if offset + 4 + length > len(data):
                return None  # Not enough data

            # Read the actual string bytes
            string_data = data[offset + 4:offset + 4 + length]
            decoded = string_data.decode('utf-8', errors='ignore').rstrip('\x00').strip()

            return decoded if decoded else None

        except Exception as e:
            logger.debug(f'Failed to read string at offset {offset}: {e}')
            return None
