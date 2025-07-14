"""
Transformation service for webhook payload transformations.
"""

import asyncio
import logging
import uuid
from typing import Dict, Any

import pandas as pd

from ..database import DatabaseConnection

logger = logging.getLogger(__name__)


class TransformationService:
    """Service for transforming webhook payloads"""

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    async def execute_transform(self, webhook_id: str, transform_query: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a transform query on a payload

        Args:
            webhook_id: ID of the webhook configuration
            transform_query: SQL query for transformation
            payload: The raw JSON payload

        Returns:
            Transformed payload
        """
        logger.info(f"Executing transform for webhook {webhook_id}")

        # Create a unique temporary table name for this transformation
        temp_table_name = f"temp_payload_{str(uuid.uuid4()).replace('-', '_')}"

        # Convert payload to DataFrame - flatten if it's a nested JSON
        if isinstance(payload, dict):
            df = pd.DataFrame([payload])
        else:
            df = pd.DataFrame(payload)

        # Register the DataFrame as a temporary table
        async with self.db_connection.query_lock:
            self.db_connection.connection.register(temp_table_name, df)

        try:
            # Execute the transform query
            modified_query = transform_query.replace("{{payload}}", temp_table_name)
            # Pass an empty dict to ensure we handle parameters consistently
            result = await self.db_connection.execute_query(modified_query, {})

            # If the result is empty, return an empty dict
            if not result or not result[0]:
                return {}

            # Get column names from the result query
            result_columns = None
            async with self.db_connection.query_lock:
                try:
                    # Run the query with a LIMIT 0 to get column names
                    temp_query = f"SELECT * FROM ({modified_query}) LIMIT 0"
                    result_columns = self.db_connection.connection.execute(temp_query).description
                except Exception as e:
                    logger.error(f"Error getting result columns: {e}")
                    # Fall back to using the first row keys and values
                    return {f"col_{i}": val for i, val in enumerate(result[0])}

            if not result_columns:
                return {}

            column_names = [col[0] for col in result_columns]

            # Convert result to dict
            if len(result) == 1:
                # Single row result - return as a flat dictionary
                result_dict = {}
                for i, col in enumerate(column_names):
                    if i < len(result[0]):
                        result_dict[col] = result[0][i]
                return result_dict
            else:
                # Multiple row result - return as a list of dictionaries
                result_list = []
                for row in result:
                    row_dict = {}
                    for i, col in enumerate(column_names):
                        if i < len(row):
                            row_dict[col] = row[i]
                    result_list.append(row_dict)
                return {"results": result_list}

        except Exception as e:
            logger.error(f"Error executing transform: {e}")
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