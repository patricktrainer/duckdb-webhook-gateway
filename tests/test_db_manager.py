#!/usr/bin/env python3
"""
Tests for DuckDBManager class
"""

import asyncio
import json
import uuid
from datetime import datetime

import pytest
import pandas as pd

from src.app import DuckDBManager
from tests.test_helper import ensure_str


class TestDuckDBManagerInitialization:
    """Test DuckDBManager initialization and basic functionality"""

    def test_initialize_db_creates_tables(self, db_manager):
        """Test that initialization creates the required tables"""
        # Check if the required tables exist
        tables = db_manager.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [row[0] for row in tables]

        assert "webhooks" in table_names
        assert "raw_events" in table_names
        assert "transformed_events" in table_names
        assert "reference_tables" in table_names
        assert "python_udfs" in table_names

    def test_db_connection_is_working(self, db_manager):
        """Test that the database connection is working"""
        # Execute a simple query
        result = db_manager.connection.execute("SELECT 1").fetchone()
        assert result[0] == 1


class TestDuckDBManagerQueries:
    """Test DuckDBManager query methods"""

    @pytest.mark.asyncio
    async def test_execute_query_with_no_params(self, db_manager):
        """Test executing a query with no parameters"""
        result = await db_manager.execute_query("SELECT 1")
        assert result[0][0] == 1

    @pytest.mark.asyncio
    async def test_execute_query_with_dict_params(self, db_manager):
        """Test executing a query with dictionary parameters"""
        result = await db_manager.execute_query(
            "SELECT ? AS value", {"value": 42}
        )
        assert result[0][0] == 42

    @pytest.mark.asyncio
    async def test_execute_query_with_numbered_params(self, db_manager):
        """Test executing a query with numbered parameters"""
        result = await db_manager.execute_query(
            "SELECT ? AS first, ? AS second", {1: "one", 2: "two"}
        )
        assert result[0][0] == "one"
        assert result[0][1] == "two"

    @pytest.mark.asyncio
    async def test_execute_query_with_tuple_params(self, db_manager):
        """Test executing a query with tuple parameters"""
        result = await db_manager.execute_query(
            "SELECT ? AS value", ("test",)
        )
        assert result[0][0] == "test"

    @pytest.mark.asyncio
    async def test_execute_query_error_handling(self, db_manager):
        """Test error handling in execute_query"""
        with pytest.raises(Exception):
            await db_manager.execute_query("SELECT * FROM nonexistent_table")


class TestDuckDBManagerEvents:
    """Test DuckDBManager event logging methods"""

    @pytest.mark.asyncio
    async def test_log_raw_event(self, db_manager, sample_payload):
        """Test logging a raw webhook event"""
        source_path = "/test-path"
        event_id = await db_manager.log_raw_event(source_path, sample_payload)

        # Check if the event was logged properly
        result = await db_manager.execute_query(
            "SELECT id, source_path, payload FROM raw_events WHERE id = ?",
            {"id": event_id}
        )

        assert ensure_str(result[0][0]) == ensure_str(event_id)
        assert result[0][1] == source_path
        assert json.loads(result[0][2]) == sample_payload

    @pytest.mark.asyncio
    async def test_log_transformed_event(self, db_manager, sample_payload, sample_webhook_config):
        """Test logging a transformed webhook event"""
        # First log a raw event
        raw_event_id = await db_manager.log_raw_event("/test-path", sample_payload)

        # Create a real webhook so foreign key constraints are satisfied
        webhook = await db_manager.register_webhook(sample_webhook_config)
        webhook_id = webhook["id"]

        # Log a transformed event
        transformed_payload = {"transformed": True, "data": "test"}
        destination_url = "https://example.com/webhook"
        success = True
        response_code = 200
        response_body = '{"status": "OK"}'

        event_id = await db_manager.log_transformed_event(
            raw_event_id,
            webhook_id,
            transformed_payload,
            destination_url,
            success,
            response_code,
            response_body
        )

        # Check if the event was logged properly
        result = await db_manager.execute_query(
            """
            SELECT id, raw_event_id, webhook_id, transformed_payload,
                   destination_url, success, response_code, response_body
            FROM transformed_events
            WHERE id = ?
            """,
            {"id": event_id}
        )

        assert ensure_str(result[0][0]) == ensure_str(event_id)
        assert ensure_str(result[0][1]) == ensure_str(raw_event_id)
        assert ensure_str(result[0][2]) == ensure_str(webhook_id)
        assert json.loads(result[0][3]) == transformed_payload
        assert result[0][4] == destination_url
        assert result[0][5] == success
        assert result[0][6] == response_code
        assert result[0][7] == response_body