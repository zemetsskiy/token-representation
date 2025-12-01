import unittest
import polars as pl
import math
from src.processors.price_calculator import PriceCalculator

# Mock Constants
SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
USDC_ADDRESS = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'

class TestPriceCalculator(unittest.TestCase):
    def setUp(self):
        # Mock DB client (not used in get_prices_for_chunk)
        self.calculator = PriceCalculator(None)
        self.calculator.set_sol_price(190.0)

    def test_sol_pair_calculation(self):
        """Test calculation for Token/SOL pair"""
        # Scenario: 
        # Token (9 decimals)
        # Pool: 100 real Token / 1 real SOL
        # Raw: 100 * 1e9 / 1 * 1e9
        
        price_data = [{
            'token': 'TokenA',
            'price_reference': SOL_ADDRESS,
            'base_coin': 'TokenA',
            'quote_coin': SOL_ADDRESS,
            'base_balance': 100 * 1e9,
            'quote_balance': 1 * 1e9
        }]
        
        decimals_map = {'TokenA': 9}
        
        df = self.calculator.get_prices_for_chunk(price_data, decimals_map)
        
        # Expected:
        # Price in SOL = 1 / 100 = 0.01
        # Price in USD = 0.01 * 190 = 1.90
        
        row = df.filter(pl.col('mint') == 'TokenA').row(0, named=True)
        self.assertAlmostEqual(row['price_in_sol'], 0.01)
        self.assertAlmostEqual(row['price_usd'], 1.90)

    def test_usdc_pair_calculation(self):
        """Test calculation for Token/USDC pair"""
        # Scenario:
        # Token (6 decimals)
        # Pool: 100 real Token / 200 real USDC
        # Raw: 100 * 1e6 / 200 * 1e6
        
        price_data = [{
            'token': 'TokenB',
            'price_reference': USDC_ADDRESS,
            'base_coin': 'TokenB',
            'quote_coin': USDC_ADDRESS,
            'base_balance': 100 * 1e6,
            'quote_balance': 200 * 1e6
        }]
        
        decimals_map = {'TokenB': 6}
        
        df = self.calculator.get_prices_for_chunk(price_data, decimals_map)
        
        # Expected:
        # Price per Ref (USDC) = 200 / 100 = 2.0
        # Price USD = 2.0
        
        row = df.filter(pl.col('mint') == 'TokenB').row(0, named=True)
        self.assertAlmostEqual(row['price_usd'], 2.0)

    def test_decimals_mismatch(self):
        """Test calculation with different decimals"""
        # Scenario:
        # Token (5 decimals)
        # USDC (6 decimals)
        # Pool: 0.1 real Token / 2.0 real USDC
        # Token Raw = 0.1 * 1e5 = 10,000
        # USDC Raw = 2.0 * 1e6 = 2,000,000
        
        price_data = [{
            'token': 'TokenC',
            'price_reference': USDC_ADDRESS,
            'base_coin': 'TokenC',
            'quote_coin': USDC_ADDRESS,
            'base_balance': 10000.0,
            'quote_balance': 2000000.0
        }]
        
        decimals_map = {'TokenC': 5}
        
        df = self.calculator.get_prices_for_chunk(price_data, decimals_map)
        
        # Expected:
        # Price = 2.0 / 0.1 = 20.0
        
        row = df.filter(pl.col('mint') == 'TokenC').row(0, named=True)
        self.assertAlmostEqual(row['price_usd'], 20.0)

    def test_inverse_pair(self):
        """Test when Token is Quote and Reference is Base"""
        # Scenario:
        # Base: SOL (Ref)
        # Quote: Token (9 dec)
        # Pool: 1 SOL / 100 Token
        
        price_data = [{
            'token': 'TokenD',
            'price_reference': SOL_ADDRESS,
            'base_coin': SOL_ADDRESS,
            'quote_coin': 'TokenD',
            'base_balance': 1 * 1e9,
            'quote_balance': 100 * 1e9
        }]
        
        decimals_map = {'TokenD': 9}
        
        df = self.calculator.get_prices_for_chunk(price_data, decimals_map)
        
        # Expected:
        # Price = RefAmount / TokenAmount = 1 / 100 = 0.01 SOL
        
        row = df.filter(pl.col('mint') == 'TokenD').row(0, named=True)
        self.assertAlmostEqual(row['price_in_sol'], 0.01)

if __name__ == '__main__':
    unittest.main()
