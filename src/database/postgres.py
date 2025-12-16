import logging
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
from datetime import datetime
import polars as pl
from ..config import Config

logger = logging.getLogger(__name__)


class PostgresClient:
    """
    PostgreSQL client for storing processed token metrics.
    """

    def __init__(self):
        self.connection = None
        self.cursor = None
        self._connect()

    def _connect(self):
        """Establish connection to PostgreSQL."""
        try:
            # Use connection string if provided, otherwise use individual parameters
            if Config.POSTGRES_CONNECTION_STRING:
                logger.info('Connecting to PostgreSQL using connection string')
                self.connection = psycopg2.connect(
                    Config.POSTGRES_CONNECTION_STRING,
                    connect_timeout=10
                )
                # Extract database name from connection string for logging
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(Config.POSTGRES_CONNECTION_STRING)
                    db_info = f'{parsed.hostname}:{parsed.port}/{parsed.path.lstrip("/")}'
                except:
                    db_info = 'connection string'
                logger.info(f'Connected to PostgreSQL at {db_info}')
            else:
                logger.info('Connecting to PostgreSQL using individual parameters')
                self.connection = psycopg2.connect(
                    host=Config.POSTGRES_HOST,
                    port=Config.POSTGRES_PORT,
                    database=Config.POSTGRES_DATABASE,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                    connect_timeout=10
                )
                logger.info(f'Connected to PostgreSQL at {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}')

            self.connection.autocommit = False
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self._ensure_table_exists()
        except Exception as e:
            logger.error(f'Failed to connect to PostgreSQL: {e}')
            raise

    def _ensure_table_exists(self):
        """Create unverified_tokens table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS unverified_tokens (
            id BIGSERIAL PRIMARY KEY,
            contract_address VARCHAR(48) NOT NULL,
            chain VARCHAR(50) NOT NULL,
            decimals INTEGER,
            symbol VARCHAR(20),
            name VARCHAR(255),
            price_usd DOUBLE PRECISION DEFAULT 0,
            market_cap_usd DOUBLE PRECISION DEFAULT 0,
            supply DOUBLE PRECISION DEFAULT 0,
            largest_lp_pool_usd DOUBLE PRECISION DEFAULT 0,
            first_tx_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            view_source VARCHAR(100),
            CONSTRAINT unique_token_chain UNIQUE (contract_address, chain)
        );

        CREATE INDEX IF NOT EXISTS idx_unverified_contract_address ON unverified_tokens(contract_address);
        CREATE INDEX IF NOT EXISTS idx_unverified_chain ON unverified_tokens(chain);
        CREATE INDEX IF NOT EXISTS idx_unverified_updated_at ON unverified_tokens(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_unverified_contract_chain ON unverified_tokens(contract_address, chain);
        """
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("Ensured unverified_tokens table exists")
        except Exception as e:
            logger.error(f"Failed to create unverified_tokens table: {e}")
            self.connection.rollback()
            raise

    def _reconnect(self):
        """Reconnect to PostgreSQL."""
        logger.warning('Reconnecting to PostgreSQL...')
        self.close()
        self._connect()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as list of dicts.

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            List of result rows as dictionaries
        """
        try:
            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description]
            results = []
            for row in self.cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results
        except Exception as e:
            logger.error(f'Query execution failed: {e}')
            self.connection.rollback()
            raise

    def _get_known_decimals(self, tokens: List[tuple]) -> Dict[tuple, int]:
        """
        Efficiently fetch known decimals for a batch of tokens.

        Args:
            tokens: List of (contract_address, chain) tuples

        Returns:
            Dict mapping (contract_address, chain) -> decimals
        """
        if not tokens:
            return {}

        # Build query with VALUES for efficient IN clause
        query = """
        WITH input_tokens (contract_address, chain) AS (
            VALUES %s
        )
        SELECT DISTINCT ON (t.contract_address, t.chain)
            t.contract_address,
            t.chain,
            tm.decimals
        FROM input_tokens t
        JOIN unverified_tokens tm
            ON tm.contract_address = t.contract_address
            AND tm.chain = t.chain
        WHERE tm.decimals IS NOT NULL
        ORDER BY t.contract_address, t.chain, tm.updated_at DESC
        """

        try:
            # Use execute_values for efficient batch query
            from psycopg2.extras import execute_values

            with self.connection.cursor() as cur:
                execute_values(
                    cur,
                    query,
                    tokens,
                    template="(%s, %s)",
                    page_size=1000
                )
                results = cur.fetchall()

            # Build dict for O(1) lookup
            decimals_map = {
                (row[0], row[1]): row[2]
                for row in results
            }

            if decimals_map:
                logger.info(f'Found existing decimals for {len(decimals_map):,} tokens')

            return decimals_map

        except Exception as e:
            logger.warning(f'Failed to fetch known decimals: {e}')
            # Rollback to recover from error state
            try:
                self.connection.rollback()
            except Exception:
                pass
            return {}

    def insert_token_metrics_batch(
        self,
        df: pl.DataFrame,
        view_source: str,
        batch_size: int = 1000
    ) -> int:
        """
        Upsert token metrics from Polars DataFrame in batches.

        UPSERT logic:
        - If token (contract_address, chain) exists: UPDATE all fields EXCEPT decimals and first_tx_date
        - If token doesn't exist: INSERT new record
        - Preserves existing decimals and first_tx_date values (only set once, never overwritten)
        - Updates: price_usd, market_cap_usd, supply, largest_lp_pool_usd, symbol, name, view_source, updated_at

        Args:
            df: Polars DataFrame with token metrics
            view_source: Source view name (e.g., 'sol_500_swaps_7_days')
            batch_size: Number of rows per batch

        Returns:
            Number of rows upserted
        """
        logger.info(f'Upserting {len(df):,} token metrics from {view_source}')

        # Step 1: Get all unique (contract_address, chain) pairs from DataFrame
        unique_tokens = []
        for row in df.iter_rows(named=True):
            token_key = (
                row.get('mint'),
                row.get('chain', row.get('blockchain', 'solana'))
            )
            unique_tokens.append(token_key)

        # Remove duplicates while preserving order
        unique_tokens = list(dict.fromkeys(unique_tokens))

        # Step 2: Fetch known decimals for these tokens (single efficient query)
        known_decimals = self._get_known_decimals(unique_tokens)

        # Prepare data for insertion with UPSERT logic
        upsert_query = """
        INSERT INTO unverified_tokens (
            contract_address,
            chain,
            decimals,
            symbol,
            name,
            price_usd,
            market_cap_usd,
            supply,
            largest_lp_pool_usd,
            first_tx_date,
            view_source,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (contract_address, chain)
        DO UPDATE SET
            decimals = COALESCE(unverified_tokens.decimals, EXCLUDED.decimals),
            first_tx_date = COALESCE(unverified_tokens.first_tx_date, EXCLUDED.first_tx_date),
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            supply = EXCLUDED.supply,
            largest_lp_pool_usd = EXCLUDED.largest_lp_pool_usd,
            view_source = EXCLUDED.view_source,
            updated_at = EXCLUDED.updated_at
        """

        total_inserted = 0
        update_time = datetime.utcnow()

        try:
            # Step 3: Convert DataFrame to list of tuples, using known decimals
            rows = []
            decimals_preserved = 0
            decimals_new = 0

            for row in df.iter_rows(named=True):
                contract_addr = row.get('mint')
                chain = row.get('chain', row.get('blockchain', 'solana'))
                token_key = (contract_addr, chain)

                # Use existing decimals if available, otherwise use new value
                existing_decimals = known_decimals.get(token_key)
                new_decimals = row.get('decimals')

                if existing_decimals is not None:
                    decimals_to_use = existing_decimals
                    decimals_preserved += 1
                else:
                    decimals_to_use = new_decimals
                    if new_decimals is not None:
                        decimals_new += 1

                rows.append((
                    contract_addr,
                    chain,
                    decimals_to_use,
                    row.get('symbol'),
                    row.get('name'),
                    float(row.get('price_usd', 0) or 0),
                    float(row.get('market_cap_usd', 0) or 0),
                    float(row.get('supply', 0) or 0),
                    float(row.get('largest_lp_pool_usd', 0) or 0),
                    row.get('first_tx_date'),
                    view_source,
                    update_time
                ))

            if decimals_preserved > 0:
                logger.info(f'  Preserved existing decimals for {decimals_preserved:,} tokens')
            if decimals_new > 0:
                logger.info(f'  Using new decimals for {decimals_new:,} tokens')

            # Insert/Update in batches
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                self.cursor.executemany(upsert_query, batch)
                self.connection.commit()
                total_inserted += len(batch)
                logger.info(f'  Upserted batch {i // batch_size + 1}: {len(batch)} rows (total: {total_inserted:,})')

            logger.info(f'Successfully upserted {total_inserted:,} token metrics')
            return total_inserted

        except Exception as e:
            logger.error(f'Failed to upsert token metrics: {e}', exc_info=True)
            self.connection.rollback()
            raise

    def upsert_token_metrics_batch(
        self,
        df: pl.DataFrame,
        view_source: str,
        batch_size: int = 1000
    ) -> int:
        """
        Upsert (insert or update) token metrics from Polars DataFrame.
        Uses ON CONFLICT to update existing records.

        Args:
            df: Polars DataFrame with token metrics
            view_source: Source view name
            batch_size: Number of rows per batch

        Returns:
            Number of rows upserted
        """
        logger.info(f'Upserting {len(df):,} token metrics from {view_source}')

        upsert_query = """
        INSERT INTO unverified_tokens (
            contract_address,
            chain,
            decimals,
            symbol,
            name,
            price_usd,
            market_cap_usd,
            supply,
            largest_lp_pool_usd,
            first_tx_date,
            view_source,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (contract_address, chain, updated_at)
        DO UPDATE SET
            decimals = EXCLUDED.decimals,
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            supply = EXCLUDED.supply,
            largest_lp_pool_usd = EXCLUDED.largest_lp_pool_usd,
            first_tx_date = EXCLUDED.first_tx_date,
            view_source = EXCLUDED.view_source
        """

        total_upserted = 0
        update_time = datetime.utcnow()

        try:
            rows = []
            for row in df.iter_rows(named=True):
                rows.append((
                    row.get('mint'),
                    row.get('chain', row.get('blockchain', 'solana')),
                    row.get('decimals'),
                    row.get('symbol'),
                    row.get('name'),
                    float(row.get('price_usd', 0) or 0),
                    float(row.get('market_cap_usd', 0) or 0),
                    float(row.get('supply', 0) or 0),
                    float(row.get('largest_lp_pool_usd', 0) or 0),
                    row.get('first_tx_date'),
                    view_source,
                    update_time
                ))

            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                self.cursor.executemany(upsert_query, batch)
                self.connection.commit()
                total_upserted += len(batch)
                logger.info(f'  Upserted batch {i // batch_size + 1}: {len(batch)} rows (total: {total_upserted:,})')

            logger.info(f'Successfully upserted {total_upserted:,} token metrics')
            return total_upserted

        except Exception as e:
            logger.error(f'Failed to upsert token metrics: {e}', exc_info=True)
            self.connection.rollback()
            raise

    def get_latest_metrics(self, contract_address: str, chain: str = 'solana') -> Optional[Dict[str, Any]]:
        """
        Get latest metrics for a specific token.

        Args:
            contract_address: Token contract address
            chain: Chain name

        Returns:
            Latest metrics as dict or None
        """
        query = """
        SELECT * FROM latest_unverified_tokens
        WHERE contract_address = %s AND chain = %s
        """
        try:
            results = self.execute_query(query, (contract_address, chain))
            return results[0] if results else None
        except Exception as e:
            logger.error(f'Failed to get latest metrics: {e}')
            return None

    def get_top_tokens_by_market_cap(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get top tokens by market cap.

        Args:
            limit: Number of tokens to return

        Returns:
            List of token metrics
        """
        query = """
        SELECT * FROM latest_unverified_tokens
        WHERE market_cap_usd > 0
        ORDER BY market_cap_usd DESC
        LIMIT %s
        """
        try:
            return self.execute_query(query, (limit,))
        except Exception as e:
            logger.error(f'Failed to get top tokens: {e}')
            return []

    def get_metrics_count(self) -> int:
        """Get total number of metric records."""
        query = "SELECT COUNT(*) as count FROM unverified_tokens"
        try:
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f'Failed to get metrics count: {e}')
            return 0

    def refresh_materialized_views(self):
        """Refresh all materialized views (currently none for unverified_tokens)."""
        logger.info('No materialized views to refresh for unverified_tokens')
        pass

    def vacuum_analyze(self):
        """Run VACUUM ANALYZE to optimize table."""
        try:
            logger.info('Running VACUUM ANALYZE...')
            old_autocommit = self.connection.autocommit
            self.connection.autocommit = True
            self.cursor.execute('VACUUM ANALYZE unverified_tokens')
            self.connection.autocommit = old_autocommit
            logger.info('VACUUM ANALYZE completed')
        except Exception as e:
            logger.error(f'Failed to run VACUUM ANALYZE: {e}')

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info('PostgreSQL connection closed')


# Singleton instance
_postgres_client = None


def get_postgres_client() -> PostgresClient:
    """Get or create PostgreSQL client singleton."""
    global _postgres_client
    if _postgres_client is None:
        _postgres_client = PostgresClient()
    return _postgres_client
