"""
Filter service for webhook payload filtering.
"""

import logging
import uuid
from typing import Dict, Any

import pandas as pd

from ..database import DatabaseConnection

logger = logging.getLogger(__name__)


class FilterService:
    """Service for filtering webhook payloads"""

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    async def apply_filter(self, webhook_id: str, filter_query: str, payload: Dict[str, Any]) -> bool:
        """Apply a filter query to a payload to determine if it should be forwarded

        Args:
            webhook_id: ID of the webhook configuration
            filter_query: SQL WHERE condition for filtering
            payload: The raw JSON payload

        Returns:
            True if the payload passes the filter, False otherwise
        """
        logger.info(f"Applying filter for webhook {webhook_id}")

        if not filter_query:
            return True

        # Create a temporary table with the payload
        temp_table_name = f"temp_filter_{str(uuid.uuid4()).replace('-', '_')}"

        # Convert payload to DataFrame
        if isinstance(payload, dict):
            df = pd.DataFrame([payload])
        else:
            df = pd.DataFrame(payload)

        # Register the DataFrame as a temporary table
        async with self.db_connection.query_lock:
            self.db_connection.connection.register(temp_table_name, df)

        try:
            # Execute the filter query
            modified_query = f"SELECT COUNT(*) FROM {temp_table_name} WHERE {filter_query}"
            # Pass an empty tuple since we don't have parameters to bind
            result = await self.db_connection.execute_query(modified_query, {})

            # If at least one row passes the filter, return True
            passes_filter = result[0][0] > 0
            logger.info(f"Filter result for webhook {webhook_id}: {passes_filter}")
            return passes_filter
        except Exception as e:
            logger.error(f"Error applying filter: {e}")
            raise
        finally:
            # Unregister the temporary view instead of dropping it as a table
            # The connection.register method creates a view, not a table
            async with self.db_connection.query_lock:
                try:
                    self.db_connection.connection.unregister(temp_table_name)
                    logger.debug(f"Successfully unregistered view {temp_table_name}")
                except Exception as e:
                    logger.error(f"Error unregistering view {temp_table_name}: {e}")
                    # If unregister fails, try to drop as a view (fallback)
                    try:
                        await self.db_connection.execute_query(f"DROP VIEW IF EXISTS {temp_table_name}", {})
                    except Exception as drop_err:
                        logger.error(f"Failed to drop view {temp_table_name}: {drop_err}")