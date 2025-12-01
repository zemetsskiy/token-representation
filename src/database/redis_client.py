import redis
import logging
from ..config import Config

logger = logging.getLogger(__name__)

class RedisClient:
    """
    Redis client for fetching live price data.
    """
    def __init__(self):
        self.enabled = False
        self.client = None
        
        if Config.REDIS_HOST:
            try:
                self.client = redis.Redis(
                    host=Config.REDIS_HOST,
                    port=Config.REDIS_PORT,
                    db=Config.REDIS_DB,
                    password=Config.REDIS_PASSWORD,
                    decode_responses=True,
                    socket_timeout=2.0  # Short timeout to avoid blocking
                )
                self.client.ping()
                self.enabled = True
                logger.info(f"Connected to Redis at {Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}")
            except Exception as e:
                logger.warning(f"Failed to connect to Redis: {e}. Price feeder will be disabled.")
                self.client = None

    def get_sol_price(self) -> float | None:
        """
        Fetch live SOL price from Redis.
        Returns None if Redis is disabled or key is missing.
        """
        if not self.enabled or not self.client:
            return None

        try:
            price_str = self.client.get(Config.SOL_PRICE_KEY)
            if price_str:
                return float(price_str)
            else:
                logger.debug(f"Key {Config.SOL_PRICE_KEY} not found in Redis")
                return None
        except Exception as e:
            logger.error(f"Error fetching price from Redis: {e}")
            return None
