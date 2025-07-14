"""
Reference table and UDF repository for database operations.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

import pandas as pd

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class ReferenceRepository(BaseRepository):
    """Repository for reference table and UDF operations"""

    async def upload_reference_table(self, webhook_id: str, table_name: str,
                                   description: str, data: pd.DataFrame) -> str:
        """Upload a reference table for a webhook

        Args:
            webhook_id: ID of the webhook configuration
            table_name: Name of the reference table
            description: Description of the reference table
            data: DataFrame containing the table data

        Returns:
            Table ID
        """
        logger.info(f"Uploading reference table {table_name} for webhook {webhook_id}")

        # Check if the table already exists
        existing = await self.execute_query(
            "SELECT id, table_name FROM reference_tables WHERE webhook_id = ? AND table_name LIKE ?",
            {"webhook_id": webhook_id, "table_name": f"%{table_name}"}
        )

        table_id = str(uuid.uuid4()) if not existing else existing[0][0]
        now = datetime.now()

        # Ensure table name is safe for SQL and prefixed with webhook ID for namespace isolation
        safe_table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
        actual_table_name = f"ref_{webhook_id.replace('-', '_')}_{safe_table_name}"

        # Use a unique temporary name for interim operations
        temp_table_name = f"temp_{uuid.uuid4().hex}"

        async with self.db_connection.query_lock:
            try:
                # Create a temporary view from the DataFrame first
                self.db_connection.connection.execute("BEGIN TRANSACTION")

                # Register the DataFrame with the temporary name
                self.db_connection.connection.register(temp_table_name, data)

                # Create or replace the actual table
                self.db_connection.connection.execute(f"DROP TABLE IF EXISTS {actual_table_name}")
                self.db_connection.connection.execute(f"CREATE TABLE {actual_table_name} AS SELECT * FROM {temp_table_name}")

                # Unregister the temporary view
                try:
                    self.db_connection.connection.unregister(temp_table_name)
                except Exception as e:
                    logger.error(f"Error unregistering view {temp_table_name}: {e}")
                    self.db_connection.connection.execute(f"DROP VIEW IF EXISTS {temp_table_name}")

                # Update the metadata table appropriately
                if existing:
                    old_table_name = existing[0][1]
                    if old_table_name != actual_table_name:
                        # If table name changed, drop the old one
                        self.db_connection.connection.execute(f"DROP TABLE IF EXISTS {old_table_name}")

                    self.db_connection.connection.execute("""
                        UPDATE reference_tables
                        SET table_name = ?, description = ?, updated_at = ?
                        WHERE id = ?
                    """, (actual_table_name, description, now, table_id))
                else:
                    self.db_connection.connection.execute("""
                        INSERT INTO reference_tables (id, webhook_id, table_name, description, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (table_id, webhook_id, actual_table_name, description, now, now))

                # Commit the transaction
                self.db_connection.connection.execute("COMMIT")
                logger.info(f"Reference table {actual_table_name} created successfully")
            except Exception as e:
                # Rollback on error
                self.db_connection.connection.execute("ROLLBACK")
                # Clean up temporary objects if possible
                try:
                    self.db_connection.connection.unregister(temp_table_name)
                except Exception as unreg_err:
                    logger.error(f"Error unregistering temporary view during rollback: {unreg_err}")
                    try:
                        self.db_connection.connection.execute(f"DROP VIEW IF EXISTS {temp_table_name}")
                    except Exception as drop_err:
                        logger.error(f"Error dropping view during rollback: {drop_err}")

                logger.error(f"Error creating reference table: {e}")
                raise

        logger.info(f"Reference table {actual_table_name} uploaded with ID {table_id}")
        return table_id

    async def list_reference_tables(self, webhook_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List reference tables

        Args:
            webhook_id: Optional webhook ID to filter by

        Returns:
            List of reference tables
        """
        if webhook_id:
            result = await self.execute_query("""
                SELECT id, webhook_id, table_name, description, created_at, updated_at
                FROM reference_tables
                WHERE webhook_id = ?
                ORDER BY updated_at DESC
            """, {"webhook_id": webhook_id})
        else:
            result = await self.execute_query("""
                SELECT id, webhook_id, table_name, description, created_at, updated_at
                FROM reference_tables
                ORDER BY updated_at DESC
            """)

        tables = []
        for row in result:
            tables.append({
                "id": row[0],
                "webhook_id": row[1],
                "table_name": row[2],
                "description": row[3],
                "created_at": row[4],
                "updated_at": row[5]
            })

        return tables

    async def register_python_udf(self, webhook_id: str, function_name: str, function_code: str) -> str:
        """Register a Python UDF for use in DuckDB queries

        Args:
            webhook_id: ID of the webhook configuration
            function_name: Name of the Python function
            function_code: Python code defining the function

        Returns:
            UDF ID
        """
        logger.info(f"Registering Python UDF {function_name} for webhook {webhook_id}")

        # Check if the UDF already exists
        existing = await self.execute_query(
            "SELECT id FROM python_udfs WHERE webhook_id = ? AND function_name = ?",
            {"webhook_id": webhook_id, "function_name": function_name}
        )

        udf_id = str(uuid.uuid4())
        now = datetime.now()

        # Define a scope for the function
        local_scope = {}

        # Execute the function code to define the function in the local scope
        try:
            exec(function_code, globals(), local_scope)
        except Exception as e:
            logger.error(f"Error executing UDF code: {e}")
            raise ValueError(f"Invalid function code: {e}")

        # Get the function object
        if function_name not in local_scope:
            raise ValueError(f"Function {function_name} not found in the provided code")

        function_obj = local_scope[function_name]

        # Register the function with DuckDB
        # Prefix with webhook_id to avoid name collisions
        safe_webhook_id = webhook_id.replace('-', '_')
        duckdb_function_name = f"udf_{safe_webhook_id}_{function_name}"

        # Extract return type annotation if available
        import inspect
        return_type = None
        signature = inspect.signature(function_obj)
        if signature.return_annotation != inspect.Signature.empty:
            return_type = signature.return_annotation

        # Map Python types to DuckDB types
        type_mapping = {
            str: 'VARCHAR',
            int: 'INTEGER',
            float: 'DOUBLE',
            bool: 'BOOLEAN',
            None: 'VARCHAR'  # Default to VARCHAR if None
        }

        duckdb_return_type = type_mapping.get(return_type, 'VARCHAR')

        # Set the udf_id first, so it's the same in both DB update and function creation
        udf_id = existing[0][0] if existing else str(uuid.uuid4())

        async with self.db_connection.query_lock:
            # DuckDB doesn't support dropping scalar functions in a clean way
            # We need to create a new connection to replace existing functions
            try:
                # First approach: Create the function assuming it doesn't exist
                self.db_connection.connection.create_function(duckdb_function_name, function_obj, return_type=duckdb_return_type)
            except Exception as e:
                if "already created" in str(e):
                    # If function already exists, recreate the connection
                    logger.warning(f"Function {duckdb_function_name} already exists, recreating connection")
                    self.db_connection.connection.close()
                    self.db_connection.connection = __import__('duckdb').connect(self.db_connection.db_path)
                    # Try creating again with the new connection
                    self.db_connection.connection.create_function(duckdb_function_name, function_obj, return_type=duckdb_return_type)
                else:
                    # For other errors, re-raise
                    raise

        if existing:
            query = """
                UPDATE python_udfs
                SET function_code = ?, updated_at = ?
                WHERE id = ?
            """
            params = {
                "function_code": function_code,
                "updated_at": now,
                "id": udf_id
            }
        else:
            query = """
                INSERT INTO python_udfs (id, webhook_id, function_name, function_code, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            params = {
                "id": udf_id,
                "webhook_id": webhook_id,
                "function_name": duckdb_function_name,
                "function_code": function_code,
                "created_at": now,
                "updated_at": now
            }

        await self.execute_query(query, params)

        logger.info(f"Python UDF {function_name} registered with ID {udf_id} as {duckdb_function_name}")
        return udf_id

    async def load_webhook_udfs(self, webhook_id: str):
        """Load all UDFs for a webhook

        Args:
            webhook_id: ID of the webhook configuration
        """
        udfs = await self.execute_query(
            "SELECT function_name, function_code FROM python_udfs WHERE webhook_id = ?",
            {"webhook_id": webhook_id}
        )

        if not udfs:
            return

        for function_name, function_code in udfs:
            # Define a scope for the function
            local_scope = {}

            # Execute the function code to define the function in the local scope
            try:
                exec(function_code, globals(), local_scope)

                # Extract the base function name (without the prefix)
                base_function_name = function_name.split('_', 2)[2] if function_name.startswith('udf_') else function_name

                if base_function_name in local_scope:
                    function_obj = local_scope[base_function_name]

                    # Register the function with DuckDB
                    async with self.db_connection.query_lock:
                        try:
                            # First approach: Create the function assuming it doesn't exist
                            self.db_connection.connection.create_function(function_name, function_obj)
                        except Exception as e:
                            if "already created" in str(e):
                                # If function already exists, recreate the connection
                                logger.warning(f"Function {function_name} already exists, recreating connection")
                                self.db_connection.connection.close()
                                self.db_connection.connection = __import__('duckdb').connect(self.db_connection.db_path)
                                # Try creating again with the new connection
                                self.db_connection.connection.create_function(function_name, function_obj)
                            else:
                                # For other errors, re-raise
                                raise

                    logger.info(f"Loaded UDF {function_name} for webhook {webhook_id}")
            except Exception as e:
                logger.error(f"Error loading UDF {function_name}: {e}")

    async def list_udfs(self, webhook_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List user-defined functions

        Args:
            webhook_id: Optional webhook ID to filter by

        Returns:
            List of UDFs
        """
        if webhook_id:
            result = await self.execute_query("""
                SELECT id, webhook_id, function_name, function_code, created_at, updated_at
                FROM python_udfs
                WHERE webhook_id = ?
                ORDER BY updated_at DESC
            """, {"webhook_id": webhook_id})
        else:
            result = await self.execute_query("""
                SELECT id, webhook_id, function_name, function_code, created_at, updated_at
                FROM python_udfs
                ORDER BY updated_at DESC
            """)

        udfs = []
        for row in result:
            udfs.append({
                "id": row[0],
                "webhook_id": row[1],
                "name": row[2].split('_')[-1] if row[2].startswith('udf_') else row[2],
                "code": row[3],
                "created_at": row[4],
                "updated_at": row[5]
            })

        return udfs

    async def delete_reference_tables_for_webhook(self, webhook_id: str) -> int:
        """Delete all reference tables for a webhook

        Args:
            webhook_id: The webhook ID

        Returns:
            Number of tables deleted
        """
        # First get all table names to drop the actual tables
        tables = await self.execute_query(
            "SELECT table_name FROM reference_tables WHERE webhook_id = ?",
            {"webhook_id": webhook_id}
        )

        # Drop the actual tables
        for table in tables:
            table_name = table[0]
            await self.execute_query(f"DROP TABLE IF EXISTS {table_name}")

        # Delete the metadata
        await self.execute_query(
            "DELETE FROM reference_tables WHERE webhook_id = ?",
            {"webhook_id": webhook_id}
        )

        return len(tables)

    async def delete_udfs_for_webhook(self, webhook_id: str) -> int:
        """Delete all UDFs for a webhook

        Args:
            webhook_id: The webhook ID

        Returns:
            Number of UDFs deleted
        """
        # Get all UDFs for the webhook
        udfs = await self.execute_query(
            "SELECT function_name FROM python_udfs WHERE webhook_id = ?",
            {"webhook_id": webhook_id}
        )

        # Delete the metadata
        await self.execute_query(
            "DELETE FROM python_udfs WHERE webhook_id = ?",
            {"webhook_id": webhook_id}
        )

        return len(udfs)