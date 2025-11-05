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
            self.connection = psycopg2.connect(
                host=Config.POSTGRES_HOST,
                port=Config.POSTGRES_PORT,
                database=Config.POSTGRES_DATABASE,
                user=Config.POSTGRES_USER,
                password=Config.POSTGRES_PASSWORD,
                connect_timeout=10
            )
            self.connection.autocommit = False
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            logger.info(f'Connected to PostgreSQL at {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}')
        except Exception as e:
            logger.error(f'Failed to connect to PostgreSQL: {e}')
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

    def insert_token_metrics_batch(
        self,
        df: pl.DataFrame,
        view_source: str,
        batch_size: int = 1000
    ) -> int:
        """
        Insert token metrics from Polars DataFrame in batches.

        Args:
            df: Polars DataFrame with token metrics
            view_source: Source view name (e.g., 'sol_500_swaps_7_days')
            batch_size: Number of rows per batch

        Returns:
            Number of rows inserted
        """
        logger.info(f'Inserting {len(df):,} token metrics from {view_source}')

        # Prepare data for insertion
        insert_query = """
        INSERT INTO token_data.token_metrics (
            token_address,
            blockchain,
            symbol,
            price_usd,
            market_cap_usd,
            supply,
            burned,
            total_minted,
            total_burned,
            largest_lp_pool_usd,
            source,
            first_tx_date,
            view_source,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        total_inserted = 0
        update_time = datetime.utcnow()

        try:
            # Convert DataFrame to list of tuples
            rows = []
            for row in df.iter_rows(named=True):
                rows.append((
                    row.get('mint'),
                    row.get('blockchain', 'solana'),
                    row.get('symbol'),
                    float(row.get('price_usd', 0) or 0),
                    float(row.get('market_cap_usd', 0) or 0),
                    float(row.get('supply', 0) or 0),
                    float(row.get('burned', 0) or 0),
                    int(row.get('total_minted', 0) or 0),
                    int(row.get('total_burned', 0) or 0),
                    float(row.get('largest_lp_pool_usd', 0) or 0),
                    row.get('source'),
                    row.get('first_tx_date'),
                    view_source,
                    update_time
                ))

            # Insert in batches
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                self.cursor.executemany(insert_query, batch)
                self.connection.commit()
                total_inserted += len(batch)
                logger.info(f'  Inserted batch {i // batch_size + 1}: {len(batch)} rows (total: {total_inserted:,})')

            logger.info(f'Successfully inserted {total_inserted:,} token metrics')
            return total_inserted

        except Exception as e:
            logger.error(f'Failed to insert token metrics: {e}', exc_info=True)
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
        INSERT INTO token_data.token_metrics (
            token_address,
            blockchain,
            symbol,
            price_usd,
            market_cap_usd,
            supply,
            burned,
            total_minted,
            total_burned,
            largest_lp_pool_usd,
            source,
            first_tx_date,
            view_source,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (token_address, blockchain, updated_at)
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            supply = EXCLUDED.supply,
            burned = EXCLUDED.burned,
            total_minted = EXCLUDED.total_minted,
            total_burned = EXCLUDED.total_burned,
            largest_lp_pool_usd = EXCLUDED.largest_lp_pool_usd,
            source = EXCLUDED.source,
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
                    row.get('blockchain', 'solana'),
                    row.get('symbol'),
                    float(row.get('price_usd', 0) or 0),
                    float(row.get('market_cap_usd', 0) or 0),
                    float(row.get('supply', 0) or 0),
                    float(row.get('burned', 0) or 0),
                    int(row.get('total_minted', 0) or 0),
                    int(row.get('total_burned', 0) or 0),
                    float(row.get('largest_lp_pool_usd', 0) or 0),
                    row.get('source'),
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

    def get_latest_metrics(self, token_address: str, blockchain: str = 'solana') -> Optional[Dict[str, Any]]:
        """
        Get latest metrics for a specific token.

        Args:
            token_address: Token address
            blockchain: Blockchain name

        Returns:
            Latest metrics as dict or None
        """
        query = """
        SELECT * FROM token_data.latest_token_metrics
        WHERE token_address = %s AND blockchain = %s
        """
        try:
            results = self.execute_query(query, (token_address, blockchain))
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
        SELECT * FROM token_data.latest_token_metrics
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
        query = "SELECT COUNT(*) as count FROM token_data.token_metrics"
        try:
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f'Failed to get metrics count: {e}')
            return 0

    def refresh_materialized_views(self):
        """Refresh all materialized views."""
        try:
            logger.info('Refreshing materialized views...')
            self.cursor.execute('REFRESH MATERIALIZED VIEW token_data.top_tokens_by_market_cap')
            self.connection.commit()
            logger.info('Materialized views refreshed successfully')
        except Exception as e:
            logger.error(f'Failed to refresh materialized views: {e}')
            self.connection.rollback()

    def vacuum_analyze(self):
        """Run VACUUM ANALYZE to optimize table."""
        try:
            logger.info('Running VACUUM ANALYZE...')
            old_autocommit = self.connection.autocommit
            self.connection.autocommit = True
            self.cursor.execute('VACUUM ANALYZE token_data.token_metrics')
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
