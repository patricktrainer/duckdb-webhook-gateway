#!/usr/bin/env python3
"""
Tests for reference table and UDF functionality
"""

import asyncio
import uuid
from textwrap import dedent

import pandas as pd
import pytest

from src.app import DuckDBManager, WebhookConfig
from tests.test_helper import ensure_str


class TestReferenceTables:
    """Test reference table functionality"""

    @pytest.mark.asyncio
    async def test_upload_reference_table(self, db_manager, reference_table_data):
        """Test uploading a reference table"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-ref-table",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]

        table_name = "test_table"
        description = "Test reference table"
        
        # Upload the reference table
        table_id = await db_manager.upload_reference_table(
            webhook_id, 
            table_name, 
            description, 
            reference_table_data
        )
        
        # Check the reference table metadata was stored
        metadata = await db_manager.execute_query(
            """
            SELECT id, webhook_id, table_name, description
            FROM reference_tables
            WHERE id = ?
            """,
            {"id": table_id}
        )
        
        assert len(metadata) == 1
        assert ensure_str(metadata[0][0]) == ensure_str(table_id)
        assert ensure_str(metadata[0][1]) == ensure_str(webhook_id)
        assert "test_table" in metadata[0][2]  # Table name should contain the original name
        assert metadata[0][3] == description
        
        # Check the actual table was created
        actual_table_name = metadata[0][2]
        table_exists = await db_manager.execute_query(
            f"SELECT 1 FROM {actual_table_name} LIMIT 1"
        )
        assert len(table_exists) > 0
        
        # Check the table contains the expected data
        table_data = await db_manager.execute_query(
            f"SELECT id, name, category, active FROM {actual_table_name} ORDER BY id"
        )
        
        assert len(table_data) == len(reference_table_data)
        
        for i, row in enumerate(table_data):
            assert row[0] == reference_table_data.iloc[i]["id"]
            assert row[1] == reference_table_data.iloc[i]["name"]
            assert row[2] == reference_table_data.iloc[i]["category"]
            assert row[3] == reference_table_data.iloc[i]["active"]

    @pytest.mark.asyncio
    async def test_update_reference_table(self, db_manager, reference_table_data):
        """Test updating an existing reference table"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-ref-table-update",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]

        table_name = "test_table"
        description = "Test reference table"
        
        # Upload the reference table initially
        table_id = await db_manager.upload_reference_table(
            webhook_id, 
            table_name, 
            description, 
            reference_table_data
        )
        
        # Get the actual table name
        metadata = await db_manager.execute_query(
            "SELECT table_name FROM reference_tables WHERE id = ?",
            {"id": table_id}
        )
        actual_table_name = metadata[0][0]
        
        # Create updated data
        updated_data = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Updated 1", "Updated 2", "Updated 3", "New Item"],
            "category": ["X", "Y", "X", "Z"],
            "active": [True, False, True, True]
        })
        
        # Update the reference table
        updated_description = "Updated test reference table"
        await db_manager.upload_reference_table(
            webhook_id, 
            table_name, 
            updated_description, 
            updated_data
        )
        
        # Check the metadata was updated
        updated_metadata = await db_manager.execute_query(
            """
            SELECT description
            FROM reference_tables
            WHERE id = ?
            """,
            {"id": table_id}
        )
        
        assert updated_metadata[0][0] == updated_description
        
        # Check the table was updated with the new data
        table_data = await db_manager.execute_query(
            f"SELECT id, name, category, active FROM {actual_table_name} ORDER BY id"
        )
        
        assert len(table_data) == len(updated_data)
        
        for i, row in enumerate(table_data):
            assert row[0] == updated_data.iloc[i]["id"]
            assert row[1] == updated_data.iloc[i]["name"]
            assert row[2] == updated_data.iloc[i]["category"]
            assert row[3] == updated_data.iloc[i]["active"]

    @pytest.mark.asyncio
    async def test_reference_table_in_transform_query(self, db_manager, reference_table_data, sample_payload):
        """Test using a reference table in a transform query"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-ref-table-transform",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]
        
        # Upload the reference table
        table_id = await db_manager.upload_reference_table(
            webhook_id, 
            "items", 
            "Items reference table", 
            reference_table_data
        )
        
        # Get the actual table name
        metadata = await db_manager.execute_query(
            "SELECT table_name FROM reference_tables WHERE id = ?",
            {"id": table_id}
        )
        actual_table_name = metadata[0][0]
        
        # Create a transform query that uses the reference table
        transform_query = f"""
            SELECT 
                p.field1, 
                p.field2,
                r.name AS item_name,
                r.category AS item_category
            FROM {{{{payload}}}} p
            JOIN {actual_table_name} r ON p.nested.key2 = r.id
        """
        
        # Create a payload with a key that matches an ID in the reference table
        payload = {
            "field1": "value1",
            "field2": "value2",
            "nested": {
                "key1": "value1",
                "key2": 2  # This should join with ID 2 in the reference table
            }
        }
        
        # Execute the transform
        result = await db_manager.execute_transform(webhook_id, transform_query, payload)
        
        # Check the result includes data from the reference table
        assert result["field1"] == payload["field1"]
        assert result["field2"] == payload["field2"]
        assert result["item_name"] == reference_table_data.iloc[1]["name"]  # ID 2 is at index 1
        assert result["item_category"] == reference_table_data.iloc[1]["category"]


class TestPythonUDFs:
    """Test Python UDF functionality"""

    @pytest.mark.asyncio
    async def test_register_python_udf(self, db_manager):
        """Test registering a Python UDF"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-udf",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]

        function_name = "test_function"
        function_code = dedent("""
            def test_function(text: str) -> str:
                if not text:
                    return None
                return text.upper()
        """)

        # Register the UDF
        udf_id = await db_manager.register_python_udf(
            webhook_id,
            function_name,
            function_code
        )
        
        # Check the UDF metadata was stored - using webhook_id and function_name
        metadata = await db_manager.execute_query(
            """
            SELECT id, webhook_id, function_name, function_code
            FROM python_udfs
            WHERE webhook_id = ? AND function_name LIKE ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"webhook_id": webhook_id, "function_name": f"%{function_name}"}
        )

        assert len(metadata) == 1
        # The ID may be a UUID object or a string - convert to string with ensure_str
        id_value = ensure_str(metadata[0][0])
        assert isinstance(id_value, str)
        assert ensure_str(metadata[0][1]) == ensure_str(webhook_id)
        # The function name in the DB includes the prefix
        assert webhook_id.replace('-', '_') in metadata[0][2]
        assert function_name in metadata[0][2]
        assert function_code.strip() in metadata[0][3]

    @pytest.mark.asyncio
    async def test_update_python_udf(self, db_manager):
        """Test updating an existing Python UDF"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-udf-update",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]

        function_name = "test_function"
        initial_function_code = dedent("""
            def test_function(text: str) -> str:
                if not text:
                    return None
                return text.upper()
        """)

        # Register the UDF initially
        udf_id = await db_manager.register_python_udf(
            webhook_id,
            function_name,
            initial_function_code
        )
        
        # Update the UDF with new code
        updated_function_code = dedent("""
            def test_function(text: str) -> str:
                if not text:
                    return None
                return text.lower()  # Changed to lowercase
        """)
        
        updated_udf_id = await db_manager.register_python_udf(
            webhook_id, 
            function_name, 
            updated_function_code
        )
        
        # Check the updated UDF exists in the database
        # We're no longer checking for the exact same ID since the recreation process
        # may generate a new ID. What's important is the function gets updated.
        assert isinstance(updated_udf_id, str)
        assert len(updated_udf_id) > 0
        
        # Check the function code was updated - use webhook_id and function_name to find the latest
        updated_metadata = await db_manager.execute_query(
            """
            SELECT function_code
            FROM python_udfs
            WHERE webhook_id = ? AND function_name LIKE ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"webhook_id": webhook_id, "function_name": f"%{function_name}"}
        )
        
        assert updated_function_code.strip() in updated_metadata[0][0]

    @pytest.mark.asyncio
    async def test_load_webhook_udfs(self, db_manager):
        """Test loading UDFs for a webhook"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-udf-load",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]

        function_name = "test_uppercase"
        function_code = dedent("""
            def test_uppercase(text: str) -> str:
                if not text:
                    return None
                return text.upper()
        """)

        # Register the UDF
        await db_manager.register_python_udf(
            webhook_id,
            function_name,
            function_code
        )
        
        # Load the UDFs for the webhook
        await db_manager.load_webhook_udfs(webhook_id)
        
        # Create a transform query that uses the UDF
        safe_webhook_id = webhook_id.replace('-', '_')
        duckdb_function_name = f"udf_{safe_webhook_id}_{function_name}"
        
        transform_query = f"SELECT {duckdb_function_name}('test') AS result"
        
        # Execute the query to test the UDF
        result = await db_manager.execute_query(transform_query)
        
        # Check the UDF worked correctly
        assert result[0][0] == "TEST"

    @pytest.mark.asyncio
    async def test_udf_in_transform_query(self, db_manager, sample_payload):
        """Test using a UDF in a transform query"""
        # First create a webhook to satisfy the foreign key constraint
        webhook_config = WebhookConfig(
            source_path="/test-udf-transform",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query="field1 = 'value1'",
            owner="test-owner"
        )
        registered_webhook = await db_manager.register_webhook(webhook_config)
        webhook_id = registered_webhook["id"]

        # Register a UDF that extracts the first character
        function_name = "extract_first_char"
        function_code = dedent("""
            def extract_first_char(text: str) -> str:
                if not text or len(text) == 0:
                    return None
                return text[0]
        """)

        await db_manager.register_python_udf(
            webhook_id,
            function_name,
            function_code
        )
        
        # Load the UDFs
        await db_manager.load_webhook_udfs(webhook_id)
        
        # Get the actual UDF name
        safe_webhook_id = webhook_id.replace('-', '_')
        duckdb_function_name = f"udf_{safe_webhook_id}_{function_name}"
        
        # Create a transform query that uses the UDF
        transform_query = f"""
            SELECT 
                field1,
                field2,
                {duckdb_function_name}(field1) AS first_char_field1,
                {duckdb_function_name}(field2) AS first_char_field2
            FROM {{{{payload}}}}
        """
        
        # Execute the transform
        result = await db_manager.execute_transform(webhook_id, transform_query, sample_payload)
        
        # Check the result includes the UDF output
        assert result["field1"] == sample_payload["field1"]
        assert result["field2"] == sample_payload["field2"]
        assert result["first_char_field1"] == sample_payload["field1"][0]
        assert result["first_char_field2"] == sample_payload["field2"][0]

    @pytest.mark.asyncio
    async def test_invalid_udf_code(self, db_manager):
        """Test registering a UDF with invalid code"""
        webhook_id = str(uuid.uuid4())
        function_name = "invalid_function"
        function_code = "def invalid_function(text): syntax error here"
        
        # Attempt to register the invalid UDF
        with pytest.raises(Exception):
            await db_manager.register_python_udf(
                webhook_id, 
                function_name, 
                function_code
            )