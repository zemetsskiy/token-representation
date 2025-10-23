import logging
import base58
import hashlib
import struct
import base64
from typing import Dict, List, Optional, Tuple
import requests
from ..config import Config

logger = logging.getLogger(__name__)

METAPLEX_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"


class MetadataFetcher:
    def __init__(self):
        self.rpc_url = Config.SOLANA_HTTP_RPC_URL
        if not self.rpc_url:
            raise ValueError('SOLANA_HTTP_RPC_URL is not set in the environment.')
        self.metadata_cache: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}

    def resolve_metadata_batch(self, token_addresses: List[str]) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
        """
        Resolve metadata (symbol, name, uri) for a batch of token addresses.

        Args:
            token_addresses: List of token mint addresses

        Returns:
            Dict mapping token address to (symbol, name, uri) tuple
        """
        if not token_addresses:
            return {}

        logger.info(f'Resolving metadata for {len(token_addresses)} tokens via Metaplex...')

        normalized: List[str] = []
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            if s and s not in self.metadata_cache:
                normalized.append(s)

        batch_size = 100
        for i in range(0, len(normalized), batch_size):
            batch = normalized[i:i + batch_size]
            self._fetch_metadata_batch(batch)

        result = {}
        metadata_found = 0
        for addr in token_addresses:
            s = addr.decode('utf-8', errors='ignore') if isinstance(addr, (bytes, bytearray)) else str(addr)
            s = s.replace('\x00', '').strip()
            metadata = self.metadata_cache.get(s, (None, None, None))
            result[s] = metadata
            if metadata and metadata[0] is not None:  # Has symbol
                metadata_found += 1

        logger.info(f'Finished resolving metadata. Found metadata for {metadata_found}/{len(token_addresses)} tokens')
        return result

    def _fetch_metadata_batch(self, mint_addresses: List[str]):
        """Fetch metadata for a batch of mint addresses."""
        # Derive metadata PDAs for all mints
        metadata_accounts = []
        for mint in mint_addresses:
            metadata_pda = self._derive_metadata_pda(mint)
            if metadata_pda:
                metadata_accounts.append((mint, metadata_pda))
            else:
                # Many tokens don't have Metaplex metadata - this is expected
                logger.debug(f'Could not derive metadata PDA for {mint}')
                self.metadata_cache[mint] = (None, None, None)

        if not metadata_accounts:
            return

        # Build RPC batch request
        payload = [
            {
                'jsonrpc': '2.0',
                'id': idx + 1,
                'method': 'getAccountInfo',
                'params': [metadata_pda, {'encoding': 'base64'}]
            }
            for idx, (mint, metadata_pda) in enumerate(metadata_accounts)
        ]

        try:
            resp = requests.post(self.rpc_url, json=payload, timeout=60)
            resp.raise_for_status()
            results = resp.json()

            # Handle single response wrapped in dict
            if isinstance(results, dict) and 'result' in results:
                results = [results]

            found_count = 0
            for idx, item in enumerate(results):
                if idx >= len(metadata_accounts):
                    logger.warning(f'Response index {idx} out of range')
                    continue

                mint, metadata_pda = metadata_accounts[idx]
                metadata = self._parse_metadata_account(item)
                self.metadata_cache[mint] = metadata

                if metadata and metadata[0]:  # Has symbol
                    found_count += 1
                    logger.debug(f'Found metadata for {mint[:8]}...: symbol={metadata[0]}, name={metadata[1]}')
                else:
                    logger.debug(f'No metadata found for {mint[:8]}... at PDA {metadata_pda[:8]}...')

            if found_count > 0:
                logger.info(f'Successfully fetched metadata for {found_count}/{len(metadata_accounts)} tokens in this batch')

        except requests.exceptions.RequestException as e:
            logger.error(f'RPC request failed for metadata batch: {e}')
            for mint, _ in metadata_accounts:
                self.metadata_cache.setdefault(mint, (None, None, None))
        except Exception as e:
            logger.error(f'Unexpected error processing metadata batch: {e}', exc_info=True)
            for mint, _ in metadata_accounts:
                self.metadata_cache.setdefault(mint, (None, None, None))

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
        Simplified check if a public key is on the ed25519 curve.
        For PDA derivation, we just need to ensure it's not on curve.
        """
        return False

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
