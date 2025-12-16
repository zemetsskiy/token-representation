import logging
from typing import Dict
logger = logging.getLogger(__name__)

class MarketCapCalculator:

    @staticmethod
    def calculate_market_cap(supply: int, price_usd: float) -> float:
        try:
            market_cap = supply * price_usd
            logger.debug(f'Market cap: {supply} × ${price_usd:.6f} = ${market_cap:.2f}')
            return market_cap
        except Exception as e:
            logger.error(f'Failed to calculate market cap: {e}')
            return 0.0

    @staticmethod
    def calculate_market_caps_batch(supplies: Dict[str, int], prices: Dict[str, float]) -> Dict[str, float]:
        market_caps = {}
        for token_address in supplies.keys():
            supply = supplies.get(token_address, 0)
            price = prices.get(token_address, 0.0)
            market_caps[token_address] = supply * price
        logger.info(f'Calculated market caps for {len(market_caps)} tokens')
        return market_caps