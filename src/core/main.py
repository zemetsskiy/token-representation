import logging
import time
from typing import List, Dict, Any
from datetime import datetime
from ..config import Config, setup_logging
from ..database import get_db_client, ClickHouseClient
from ..processors import TokenDiscovery, SupplyCalculator, PriceCalculator, MarketCapCalculator, LiquidityAnalyzer, FirstTxFinder, DecimalsResolver, MetadataFetcher
setup_logging()
logger = logging.getLogger(__name__)

class TokenAggregationWorker:

    def __init__(self):
        logger.info('Initializing Token Aggregation Worker')
        self.db_client = get_db_client()
        self.token_discovery = TokenDiscovery(self.db_client)
        self.supply_calculator = SupplyCalculator(self.db_client)
        self.price_calculator = PriceCalculator(self.db_client)
        self.market_cap_calculator = MarketCapCalculator()
        self.liquidity_analyzer = LiquidityAnalyzer(self.db_client)
        self.first_tx_finder = FirstTxFinder(self.db_client)
        self.decimals_resolver = DecimalsResolver()
        self.metadata_fetcher = MetadataFetcher()
        logger.info('Worker initialized successfully')

    def process_wallets(self) -> int:
        start_time = time.time()
        logger.info('Processing all token mints in the database (batch mode)')
        try:
            logger.info('Step 1/4: Discovering token mints')
            mints = self.token_discovery.discover_token_mints()
            if not mints:
                logger.warning('No mints found')
                return 0
            logger.info('Step 2/5: Resolving token decimals via RPC')
            decimals_map = self.decimals_resolver.resolve_decimals_batch(mints)
            logger.info('Step 3/5: Fetching token metadata from Metaplex')
            metadata_map = self.metadata_fetcher.resolve_metadata_batch(mints)
            logger.info('Step 4/5: Calculating supplies (batch)')
            supplies = self.supply_calculator.calculate_supplies_batch(mints, decimals_map)
            logger.info('Getting burned amounts for each token (normalized)')
            burned_amounts = {}
            for mint in mints:
                token_str = mint.decode('utf-8', errors='ignore') if isinstance(mint, (bytes, bytearray)) else str(mint)
                token_str = token_str.replace('\x00', '').strip()
                burned_raw = self.supply_calculator._get_total_burned(token_str)
                decimals = decimals_map.get(token_str, 9)
                burned_normalized = burned_raw / 10 ** decimals
                burned_amounts[token_str] = burned_normalized
            logger.info('Step 5/5: Finding first transaction dates (batch)')
            first_tx_dates = self.first_tx_finder.find_first_tx_dates_batch(mints)
            sol_price = self.price_calculator.get_sol_price()
            if sol_price:
                self.liquidity_analyzer.set_sol_price(sol_price)
            logger.info('Calculating best pool metrics (batch)')
            best_metrics = self.liquidity_analyzer.get_best_pool_metrics_batch(mints, decimals_map)
            logger.info('Computing token reserves across pools for circulating supply')
            reserves_map = self.liquidity_analyzer.get_token_reserves_map(mints, decimals_map)
            prices: Dict[str, float] = {}
            liquidities: Dict[str, float] = {}
            sources: Dict[str, str] = {}
            market_caps: Dict[str, float] = {}
            for mint in mints:
                token_str = mint.decode('utf-8', errors='ignore') if isinstance(mint, (bytes, bytearray)) else str(mint)
                token_str = token_str.replace('\x00', '').strip()
                met = best_metrics.get(token_str) or best_metrics.get(mint) or {}
                p = float(met.get('price_usd', 0.0))
                lq = float(met.get('liquidity_usd', 0.0))
                src = str(met.get('source', '')) if met else ''
                prices[token_str] = p
                liquidities[token_str] = lq
                sources[token_str] = src
                total_supply_norm = float(supplies.get(token_str, supplies.get(mint, 0.0)))
                reserves_norm = float(reserves_map.get(token_str, 0.0))
                circulating_supply = max(0.0, total_supply_norm - reserves_norm)
                sup_val = circulating_supply
                mc = float(p) * circulating_supply
                market_caps[token_str] = mc
            logger.info(f'Computed prices and market caps for {len(prices)} tokens')
            normalized_initial = self.supply_calculator.get_last_initial_minted_normalized()
            records = self._prepare_records(mints, supplies, prices, market_caps, liquidities, first_tx_dates, normalized_initial, sources, burned_amounts, metadata_map)
            if records:
                logger.info(f'Would insert {len(records)} records into database')
                self._print_records(records)
            elapsed_time = time.time() - start_time
            logger.info(f'Successfully processed {len(mints)} mints in {elapsed_time:.2f} seconds')
            return len(mints)
        except Exception as e:
            logger.error(f'Error processing wallets: {e}', exc_info=True)
            raise

    def _prepare_records(self, mints: List[str], supplies: Dict[str, int], prices: Dict[str, float], market_caps: Dict[str, float], liquidities: Dict[str, float], first_tx_dates: Dict[str, datetime], initial_minted: Dict[str, int], sources: Dict[str, str], burned_amounts: Dict[str, float], metadata_map: Dict) -> List[List[Any]]:
        records = []
        for mint in mints:
            token_str = mint.decode('utf-8', errors='ignore') if isinstance(mint, (bytes, bytearray)) else str(mint)
            token_str = token_str.replace('\x00', '').strip()
            supply = supplies.get(mint, supplies.get(token_str, 0.0))
            price_usd = prices.get(mint, prices.get(token_str, 0.0))
            market_cap_usd = market_caps.get(mint, market_caps.get(token_str, 0.0))
            largest_lp_pool_usd = liquidities.get(mint, liquidities.get(token_str, 0.0))
            first_tx_date = first_tx_dates.get(mint, first_tx_dates.get(token_str))
            source = sources.get(token_str, sources.get(mint, ''))
            burned = burned_amounts.get(token_str, burned_amounts.get(mint, 0))

            # Get metadata (symbol, name, uri)
            metadata = metadata_map.get(token_str, metadata_map.get(mint, (None, None, None)))
            symbol = metadata[0] if metadata and len(metadata) > 0 else None
            name = metadata[1] if metadata and len(metadata) > 1 else None
            uri = metadata[2] if metadata and len(metadata) > 2 else None

            if first_tx_date is None:
                logger.warning(f'Skipping token {mint[:8]}... - no first transaction date')
                continue
            record = [token_str, 'solana', symbol, price_usd, market_cap_usd, supply, burned, largest_lp_pool_usd, first_tx_date, source, name, uri]
            records.append(record)
        logger.info(f'Prepared {len(records)} records for insertion')
        return records

    def _print_records(self, records: List[List[Any]]):
        column_names = ['token_address', 'blockchain', 'symbol', 'price_usd', 'market_cap_usd', 'supply', 'burned', 'largest_lp_pool_usd', 'first_tx_date', 'source', 'name', 'uri']
        print('\n' + '=' * 100)
        print('TOKEN SUPPLY & METADATA')
        print('=' * 100)
        header = f"{'Token Address':50} {'Blockchain':12} {'Symbol':12} {'Price USD':14} {'Market Cap':18} {'Supply':14} {'Burned':14} {'Liquidity':14} {'First TX Date':19} {'Source'}"
        print(header)
        print('-' * 200)
        for record in records:
            addr = record[0]
            if isinstance(addr, (bytes, bytearray)):
                addr = addr.decode('utf-8', errors='ignore').replace('\x00', '').strip()
            token_address = addr
            blockchain = record[1]
            symbol = record[2] if record[2] else 'N/A'
            price_usd = f'${record[3]:.12f}' if record[3] > 0 else '$0.000000000000'
            market_cap = f'${record[4]:,.6f}' if record[4] > 0 else '$0'
            supply = f'{record[5]:,.6f}' if record[5] > 0 else '0'
            burned = f'{record[6]:,.6f}' if record[6] > 0 else '0'
            liquidity = f'${record[7]:,.6f}' if record[7] > 0 else '$0'
            first_tx_date = str(record[8]) if record[8] is not None else 'N/A'
            source = record[9] if record[9] else ''
            print(f'{token_address:50} {blockchain:12} {symbol:12} {price_usd:12} {market_cap:15} {supply:12} {burned:12} {liquidity:12} {first_tx_date:19} {source}')
        print('=' * 100)
        print(f'Total records: {len(records)}')
        print('=' * 100 + '\n')

    def ensure_table_exists(self):
        pass

def main():
    logger.info('=' * 80)
    logger.info('Starting Solana Token Data Aggregation Worker')
    logger.info('=' * 80)
    worker = TokenAggregationWorker()
    try:
        worker.process_wallets()
    except Exception as e:
        logger.error(f'Error processing wallets: {e}', exc_info=True)
        raise
if __name__ == '__main__':
    main()