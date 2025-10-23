import logging
import json
from typing import List, Dict, Any, Optional
from uuid import uuid4
import clickhouse_connect
from ..config import Config
logger = logging.getLogger(__name__)

class ClickHouseClient:

    def __init__(self):
        self.client = None
        self._connect()

    def _connect(self):
        try:
            self.client = clickhouse_connect.get_client(host=Config.CLICKHOUSE_HOST, port=Config.CLICKHOUSE_PORT, username=Config.CLICKHOUSE_USER, password=Config.CLICKHOUSE_PASSWORD, database=Config.CLICKHOUSE_DATABASE)
            logger.info(f'Connected to ClickHouse at {Config.CLICKHOUSE_HOST}:{Config.CLICKHOUSE_PORT}')
        except Exception as e:
            logger.error(f'Failed to connect to ClickHouse: {e}')
            raise

    def _log_query(self, query: str, parameters: Optional[Dict[str, Any]]=None):
        try:
            # Only log query summary (first 10 words) for brevity
            q = (query or '').strip()
            if q:
                query_summary = ' '.join(q.split()[:10])
                logger.debug(f'Query: {query_summary}...')
            if parameters:
                logger.debug(f'Parameters: {len(parameters)} params')
        except Exception as log_err:
            logger.debug(f'Failed to log SQL query: {log_err}')

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]]=None) -> List[tuple]:
        attempts = 2
        for attempt in range(attempts):
            try:
                self._log_query(query, parameters)
                logger.info('Executing query...')

                # Increase timeout for large aggregation queries
                settings = {
                    'session_id': str(uuid4()),
                    'session_timeout': 300,  # 5 minutes
                    'max_execution_time': 300  # 5 minutes query execution
                }

                result = self.client.query(query, parameters=parameters or {}, settings=settings)
                rows = result.result_rows

                logger.info(f'Query completed: {len(rows):,} rows')
                if rows and len(rows) <= 3:
                    logger.debug(f'Sample rows: {rows}')
                elif rows:
                    logger.debug(f'Sample (first 3): {rows[:3]}')

                return rows
            except Exception as e:
                msg = str(e)
                if ('SESSION_IS_LOCKED' in msg or 'code: 373' in msg) and attempt < attempts - 1:
                    logger.warning('Session locked, reconnecting and retrying query...')
                    self._connect()
                    continue
                logger.error(f'Query execution failed: {e}', exc_info=True)
                # Log only query summary to avoid massive logs with token lists
                query_summary = ' '.join((query or '').strip().split()[:10])
                logger.error(f'Query summary: {query_summary}...')
                raise

    def execute_query_dict(self, query: str, parameters: Optional[Dict[str, Any]]=None) -> List[Dict[str, Any]]:
        attempts = 2
        for attempt in range(attempts):
            try:
                self._log_query(query, parameters)
                logger.info('Executing query (dict)...')

                # Increase timeout for large aggregation queries
                settings = {
                    'session_id': str(uuid4()),
                    'session_timeout': 300,  # 5 minutes
                    'max_execution_time': 300  # 5 minutes query execution
                }

                result = self.client.query(query, parameters=parameters or {}, settings=settings)
                column_names = result.column_names
                dict_rows = [dict(zip(column_names, row)) for row in result.result_rows]

                logger.info(f'Query completed: {len(dict_rows):,} rows')
                if dict_rows and len(dict_rows) <= 3:
                    logger.debug(f'Sample rows: {dict_rows}')
                elif dict_rows:
                    logger.debug(f'Sample (first 3): {dict_rows[:3]}')

                return dict_rows
            except Exception as e:
                msg = str(e)
                if ('SESSION_IS_LOCKED' in msg or 'code: 373' in msg) and attempt < attempts - 1:
                    logger.warning('Session locked, reconnecting and retrying query (dict)...')
                    self._connect()
                    continue
                logger.error(f'Query execution failed: {e}', exc_info=True)
                # Log only query summary to avoid massive logs with token lists
                query_summary = ' '.join((query or '').strip().split()[:10])
                logger.error(f'Query summary: {query_summary}...')
                raise

    def execute_batch_insert(self, table: str, data: List[List[Any]], column_names: List[str]):
        try:
            if not data:
                logger.warning(f'No data to insert into {table}')
                return
            logger.info(f'Inserting {len(data)} rows into {table}')
            self.client.insert(table=table, data=data, column_names=column_names)
            logger.info(f'Successfully inserted {len(data)} rows into {table}')
        except Exception as e:
            logger.error(f'Batch insert failed: {e}')
            logger.error(f'Table: {table}, Rows: {len(data)}')
            raise

    def create_token_metrics_table(self):
        query = "\n        CREATE TABLE IF NOT EXISTS solana.token_metrics (\n            token_address FixedString(48),\n            blockchain String,\n            symbol Nullable(String),\n            name Nullable(String),\n            price_usd Float64,\n            market_cap_usd Float64,\n            supply UInt64,\n            largest_lp_pool_usd Float64,\n            first_tx_date DateTime('UTC')\n        )\n        ENGINE = MergeTree()\n        ORDER BY token_address\n        "
        attempts = 2
        for attempt in range(attempts):
            try:
                self._log_query(query)
                settings = {
                    'session_id': str(uuid4()),
                    'session_timeout': 300,  # 5 minutes
                    'max_execution_time': 300
                }
                self.client.command(query)
                logger.info('token_metrics table created or already exists')
                return
            except Exception as e:
                msg = str(e)
                if ('SESSION_IS_LOCKED' in msg or 'code: 373' in msg) and attempt < attempts - 1:
                    logger.warning('Session locked on command, reconnecting and retrying...')
                    self._connect()
                    continue
                logger.error(f'Failed to create token_metrics table: {e}', exc_info=True)
                raise

    def manage_chunk_table(self, table_name: str, data: List[List[Any]], column_names: List[str]):
        """
        Recreates and populates a temporary table for a chunk of data.
        This is the canonical ClickHouse approach for filtering large datasets.

        Args:
            table_name: Name of the temporary table (e.g., 'chunk_tokens')
            data: List of tuples/lists with data to insert
            column_names: Column names for the table
        """
        try:
            # Drop table if exists (safe even if it doesn't exist)
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            self.client.command(drop_query)
            logger.debug(f"Dropped temporary table '{table_name}' if it existed")

            # Create temporary table (ENGINE = Memory for fast in-memory operations)
            columns_def = ', '.join([f"{col} String" for col in column_names])
            create_query = f"CREATE TEMPORARY TABLE {table_name} ({columns_def}) ENGINE = Memory"
            self.client.command(create_query)
            logger.debug(f"Created temporary table '{table_name}'")

            # Insert data into temporary table
            logger.info(f"Uploading {len(data):,} rows to temporary table '{table_name}'...")
            self.client.insert(table_name, data, column_names=column_names)
            logger.info(f"Successfully uploaded data to '{table_name}'")

        except Exception as e:
            logger.error(f"Failed to manage temporary table '{table_name}': {e}", exc_info=True)
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info('ClickHouse connection closed')
_db_client = None

def get_db_client() -> ClickHouseClient:
    global _db_client
    if _db_client is None:
        _db_client = ClickHouseClient()
    return _db_client