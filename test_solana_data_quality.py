"""
Tests for Solana data quality fixes:
1. ed25519 _is_on_curve correctness
2. PDA derivation correctness
3. Supply extraction from RPC response
"""
import sys
import os
import unittest
import importlib.util

# Direct imports to avoid pulling in polars/clickhouse dependencies via __init__.py
def _import_module_direct(name, file_path):
    spec = importlib.util.spec_from_file_location(name, file_path)
    mod = importlib.util.module_from_spec(spec)
    # Stub out parent packages and config so the module can load standalone
    sys.modules.setdefault(name, mod)
    spec.loader.exec_module(mod)
    return mod

# We need to stub the config import that decimals_resolver and metadata_fetcher use
class _StubConfig:
    SOLANA_HTTP_RPC_URL = "http://dummy"

_config_mod = type(sys)("config_stub")
_config_mod.Config = _StubConfig
# The processors import from ...config — we need to set up the parent chain
sys.modules['src'] = type(sys)("src")
sys.modules['src.config'] = _config_mod
sys.modules['src.solana'] = type(sys)("src.solana")
sys.modules['src.solana.processors'] = type(sys)("src.solana.processors")

BASE = os.path.dirname(os.path.abspath(__file__))
PROC = os.path.join(BASE, "src", "solana", "processors")

# Import the actual modules directly
metadata_mod = _import_module_direct(
    "src.solana.processors.metadata_fetcher",
    os.path.join(PROC, "metadata_fetcher.py")
)
decimals_mod = _import_module_direct(
    "src.solana.processors.decimals_resolver",
    os.path.join(PROC, "decimals_resolver.py")
)

MetadataFetcher = metadata_mod.MetadataFetcher
DecimalsResolver = decimals_mod.DecimalsResolver

import base58


class TestIsOnCurve(unittest.TestCase):
    """Test that _is_on_curve correctly identifies ed25519 curve points."""

    def setUp(self):
        self.fetcher = MetadataFetcher.__new__(MetadataFetcher)
        self.fetcher.rpc_url = "http://dummy"
        self.fetcher.metadata_cache = {}

    def test_ed25519_base_point_is_on_curve(self):
        """The ed25519 base point must be detected as on-curve."""
        base_point_y = 0x6666666666666666666666666666666666666666666666666666666666666658
        base_point_bytes = base_point_y.to_bytes(32, 'little')
        base_point_bytes = bytearray(base_point_bytes)
        base_point_bytes[31] |= 0x00  # sign bit = 0
        base_point_bytes = bytes(base_point_bytes)
        self.assertTrue(self.fetcher._is_on_curve(base_point_bytes))

    def test_zeros_on_curve(self):
        """y=0: -x^2 + 0 = 1 + 0 => x^2 = -1. Since p ≡ 1 (mod 4), -1 is a QR, so (x,0) is on curve."""
        zero_bytes = b'\x00' * 32
        self.assertTrue(self.fetcher._is_on_curve(zero_bytes))

    def test_random_off_curve(self):
        """A known off-curve point should return False."""
        # Use a PDA (guaranteed off-curve by construction)
        pda = self.fetcher._derive_metadata_pda("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")
        self.assertIsNotNone(pda)
        pda_bytes = base58.b58decode(pda)
        self.assertFalse(self.fetcher._is_on_curve(pda_bytes))

    def test_known_pda_off_curve(self):
        """A known PDA (USDC metadata PDA) must NOT be on the curve."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        pda = self.fetcher._derive_metadata_pda(usdc_mint)
        self.assertIsNotNone(pda, "Failed to derive USDC metadata PDA")
        pda_bytes = base58.b58decode(pda)
        self.assertFalse(self.fetcher._is_on_curve(pda_bytes))

    def test_identity_point_on_curve(self):
        """The identity point (y=1, x=0) is on the curve."""
        identity = (1).to_bytes(32, 'little')
        self.assertTrue(self.fetcher._is_on_curve(identity))


class TestPDADerivation(unittest.TestCase):
    """Test Metaplex PDA derivation produces correct addresses."""

    def setUp(self):
        self.fetcher = MetadataFetcher.__new__(MetadataFetcher)
        self.fetcher.rpc_url = "http://dummy"
        self.fetcher.metadata_cache = {}

    def test_usdc_pda(self):
        """USDC metadata PDA should be derivable and deterministic."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        pda1 = self.fetcher._derive_metadata_pda(usdc_mint)
        pda2 = self.fetcher._derive_metadata_pda(usdc_mint)
        self.assertIsNotNone(pda1)
        self.assertEqual(pda1, pda2)  # Deterministic
        decoded = base58.b58decode(pda1)
        self.assertEqual(len(decoded), 32)
        self.assertFalse(self.fetcher._is_on_curve(decoded))  # PDA must be off-curve

    def test_sol_wrapped_pda(self):
        """Wrapped SOL metadata PDA should be derivable."""
        sol_mint = "So11111111111111111111111111111111111111112"
        pda = self.fetcher._derive_metadata_pda(sol_mint)
        self.assertIsNotNone(pda)
        decoded = base58.b58decode(pda)
        self.assertEqual(len(decoded), 32)

    def test_pda_different_mints_produce_unique_pdas(self):
        """Different mints should produce unique PDAs."""
        mints = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            "So11111111111111111111111111111111111111112",      # SOL
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
        ]
        pdas = set()
        for mint in mints:
            pda = self.fetcher._derive_metadata_pda(mint)
            self.assertIsNotNone(pda, f"Failed to derive PDA for {mint}")
            pdas.add(pda)
        self.assertEqual(len(pdas), len(mints))


class TestSupplyExtraction(unittest.TestCase):
    """Test that _parse_rpc_response correctly extracts supply."""

    def setUp(self):
        self.resolver = DecimalsResolver.__new__(DecimalsResolver)
        self.resolver.rpc_url = "http://dummy"
        self.resolver.decimals_cache = {}
        self.resolver.supply_cache = {}

    def test_parse_with_supply(self):
        """Should extract decimals and supply from a valid RPC response."""
        mock_response = {
            'jsonrpc': '2.0',
            'id': 1,
            'result': {
                'value': {
                    'data': {
                        'parsed': {
                            'info': {
                                'decimals': 9,
                                'supply': '1000000000000'
                            }
                        }
                    }
                }
            }
        }
        decimals, exists, supply = self.resolver._parse_rpc_response(mock_response)
        self.assertEqual(decimals, 9)
        self.assertTrue(exists)
        self.assertEqual(supply, 1000000000000)

    def test_parse_without_supply(self):
        """Should handle response without supply field."""
        mock_response = {
            'jsonrpc': '2.0',
            'id': 1,
            'result': {
                'value': {
                    'data': {
                        'parsed': {
                            'info': {
                                'decimals': 6
                            }
                        }
                    }
                }
            }
        }
        decimals, exists, supply = self.resolver._parse_rpc_response(mock_response)
        self.assertEqual(decimals, 6)
        self.assertTrue(exists)
        self.assertIsNone(supply)

    def test_parse_nonexistent_account(self):
        """Should return (None, False, None) for non-existent account."""
        mock_response = {
            'jsonrpc': '2.0',
            'id': 1,
            'result': {
                'value': None
            }
        }
        decimals, exists, supply = self.resolver._parse_rpc_response(mock_response)
        self.assertIsNone(decimals)
        self.assertFalse(exists)
        self.assertIsNone(supply)

    def test_parse_malformed_response(self):
        """Should handle malformed response gracefully (no 'value' key => treated as non-existent)."""
        mock_response = {'jsonrpc': '2.0', 'id': 1, 'result': {}}
        decimals, exists, supply = self.resolver._parse_rpc_response(mock_response)
        self.assertIsNone(decimals)
        self.assertFalse(exists)  # No 'value' key => account_exists=False
        self.assertIsNone(supply)

    def test_supply_zero(self):
        """Should correctly parse supply = '0'."""
        mock_response = {
            'jsonrpc': '2.0',
            'id': 1,
            'result': {
                'value': {
                    'data': {
                        'parsed': {
                            'info': {
                                'decimals': 9,
                                'supply': '0'
                            }
                        }
                    }
                }
            }
        }
        decimals, exists, supply = self.resolver._parse_rpc_response(mock_response)
        self.assertEqual(decimals, 9)
        self.assertTrue(exists)
        self.assertEqual(supply, 0)


if __name__ == '__main__':
    unittest.main()
