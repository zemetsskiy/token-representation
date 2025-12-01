import redis
import logging
from ..config import Config

logger = logging.getLogger(__name__)


class RedisConnectionError(Exception):
    """Raised when Redis connection fails and is required."""
    pass


class RedisPriceNotFoundError(Exception):
    """Raised when SOL price is not available in Redis."""
    pass


class RedisClient:
    """
    Redis client for fetching live price data.
    Redis is REQUIRED - worker cannot function without live SOL price.
    """
    def __init__(self):
        self.enabled = False
        self.client = None

        logger.info("=" * 60)
        logger.info("REDIS CONNECTION")
        logger.info("=" * 60)

        if not Config.REDIS_HOST:
            logger.error("REDIS_HOST is not configured!")
            logger.error("Redis is REQUIRED for live SOL price data.")
            logger.error("Please set REDIS_HOST in your .env file.")
            raise RedisConnectionError("REDIS_HOST is not configured. Redis is required for live SOL price.")

        logger.info(f"Connecting to Redis: {Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}")
        logger.info(f"SOL Price Key: {Config.SOL_PRICE_KEY}")

        try:
            self.client = redis.Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=Config.REDIS_DB,
                password=Config.REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=5.0
            )
            self.client.ping()
            self.enabled = True
            logger.info(f"✅ Successfully connected to Redis at {Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}")
        except redis.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            logger.error("Redis is REQUIRED for live SOL price data. Worker cannot start.")
            raise RedisConnectionError(f"Failed to connect to Redis at {Config.REDIS_HOST}:{Config.REDIS_PORT}: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to Redis: {e}")
            raise RedisConnectionError(f"Unexpected Redis error: {e}")

        logger.info("=" * 60)

    def get_sol_price(self) -> float:
        """
        Fetch live SOL price from Redis.
        Raises RedisPriceNotFoundError if price is not available.

        Returns:
            float: Current SOL price in USD

        Raises:
            RedisPriceNotFoundError: If SOL price key is not found in Redis
        """
        if not self.enabled or not self.client:
            raise RedisPriceNotFoundError("Redis client is not connected")

        try:
            price_str = self.client.get(Config.SOL_PRICE_KEY)
            if price_str:
                price = float(price_str)
                logger.info(f"✅ Got live SOL price from Redis: ${price:.2f}")
                return price
            else:
                logger.error(f"❌ Key '{Config.SOL_PRICE_KEY}' not found in Redis!")
                logger.error("SOL price is REQUIRED. Please ensure the price updater is running.")
                raise RedisPriceNotFoundError(f"Key '{Config.SOL_PRICE_KEY}' not found in Redis")
        except RedisPriceNotFoundError:
            raise
        except Exception as e:
            logger.error(f"❌ Error fetching SOL price from Redis: {e}")
            raise RedisPriceNotFoundError(f"Error fetching SOL price: {e}")
