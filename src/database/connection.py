"""
Database connection management.
"""

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages DuckDB connection with thread safety"""

    def __init__(self, db_path: str = None):
        """Initialize the database connection
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path or os.environ.get("DUCKDB_PATH", "webhook_gateway.db")
        
        # Ensure data directory exists if specified
        if os.path.dirname(self.db_path):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        logger.info(f"Connecting to DuckDB at {self.db_path}")
        self.connection = duckdb.connect(self.db_path)
        self.query_lock = asyncio.Lock()
        
        # Adjust thread pool size based on environment variable or default
        max_workers = int(os.environ.get("DUCKDB_MAX_WORKERS", "4"))
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query with the connection lock
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results
        """
        async with self.query_lock:
            return await asyncio.get_event_loop().run_in_executor(
                self.executor, self._execute_query_sync, query, params
            )
    
    def _execute_query_sync(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Synchronous execution of a query

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query results
        """
        # Begin a transaction for each query to ensure consistency
        self.connection.execute("BEGIN TRANSACTION")

        try:
            result = None
            if params:
                # If params is a dict, convert to a list or tuple for DuckDB
                if isinstance(params, dict):
                    if '?' in query and ':' not in query and '@' not in query:
                        # Convert numbered ? placeholders (positional parameters)
                        param_values = []
                        placeholder_count = query.count('?')

                        # Determine what kind of parameters we have
                        if all(isinstance(key, int) or key.isdigit() for key in params.keys()):
                            # If keys are integers or numeric strings, use them as indices
                            for i in range(1, placeholder_count + 1):
                                if i in params:
                                    param_values.append(params[i])
                                elif str(i) in params:
                                    param_values.append(params[str(i)])
                                else:
                                    raise ValueError(f"Missing parameter for placeholder {i}")
                        else:
                            # For named parameters, we need to convert them to positional
                            param_values = tuple(params.values())

                        result = self.connection.execute(query, param_values).fetchall()
                    else:
                        # Named parameters, pass as is
                        result = self.connection.execute(query, params).fetchall()
                else:
                    # If params is already a list or tuple, pass it directly
                    result = self.connection.execute(query, params).fetchall()
            else:
                result = self.connection.execute(query).fetchall()

            # Commit the transaction if the query executed successfully
            self.connection.execute("COMMIT")
            return result

        except Exception as e:
            # Rollback the transaction if there was an error
            self.connection.execute("ROLLBACK")
            logger.error(f"Error executing query: {query}")
            logger.error(f"Parameters: {params}")
            logger.error(f"Error: {e}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()