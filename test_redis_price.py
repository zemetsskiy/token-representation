import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Mock Config before importing modules that use it
with patch.dict(os.environ, {
    'REDIS_HOST': 'localhost',
    'REDIS_PORT': '6379',
    'REDIS_DB': '2',
    'SOL_PRICE_KEY': 'solana:price_usd'
}):
    from src.database.redis_client import RedisClient

class TestRedisClient(unittest.TestCase):
    
    @patch('redis.Redis')
    def test_get_price_success(self, mock_redis_cls):
        # Setup mock
        mock_client = MagicMock()
        mock_client.get.return_value = "150.50"
        mock_redis_cls.return_value = mock_client
        
        # Test
        client = RedisClient()
        price = client.get_sol_price()
        
        # Verify
        self.assertEqual(price, 150.50)
        mock_client.get.assert_called_with('solana:price_usd')

    @patch('redis.Redis')
    def test_get_price_missing_key(self, mock_redis_cls):
        # Setup mock
        mock_client = MagicMock()
        mock_client.get.return_value = None
        mock_redis_cls.return_value = mock_client
        
        # Test
        client = RedisClient()
        price = client.get_sol_price()
        
        # Verify
        self.assertIsNone(price)

    @patch('redis.Redis')
    def test_connection_failure(self, mock_redis_cls):
        # Setup mock to raise exception on init
        mock_redis_cls.side_effect = Exception("Connection refused")
        
        # Test
        client = RedisClient()
        
        # Verify
        self.assertFalse(client.enabled)
        self.assertIsNone(client.get_sol_price())

if __name__ == '__main__':
    unittest.main()
