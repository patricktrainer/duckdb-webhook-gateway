#!/usr/bin/env python3
"""
Test configuration and fixtures for DuckDB Webhook Gateway
"""

import asyncio
import os
import uuid
from datetime import datetime

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.app import app, WebhookConfig, DuckDBManager, get_api_key

# Constants for testing
TEST_DB_PATH = "test_webhook_gateway.db"
TEST_API_KEY = "test_api_key"

# Mock environment variables
os.environ["WEBHOOK_GATEWAY_API_KEY"] = TEST_API_KEY


@pytest.fixture(scope="function")
def test_db_path():
    """Return a path to a test database file"""
    # Use a unique path for each test to avoid conflicts
    return f"test_webhook_gateway_{uuid.uuid4()}.db"


@pytest.fixture(scope="function")
def db_manager(test_db_path):
    """Create a test DuckDBManager instance"""
    # Create a new DuckDBManager with a test database
    manager = DuckDBManager(db_path=test_db_path)

    # PRAGMA foreign_keys is not supported by DuckDB, so we have to handle constraints differently

    # Return the manager for use in tests
    yield manager

    # Clean up - close connection and remove test database
    manager.connection.close()
    try:
        os.remove(test_db_path)
    except FileNotFoundError:
        pass  # File might not exist if tests failed early


@pytest.fixture(scope="function")
def test_client():
    """Return a FastAPI TestClient instance with API key bypass"""
    # Override the dependency to use the test API key
    app.dependency_overrides[get_api_key] = lambda: TEST_API_KEY

    with TestClient(app) as client:
        yield client

    # Clean up
    app.dependency_overrides.clear()

@pytest.fixture(scope="function")
def test_client_no_override():
    """Return a FastAPI TestClient instance without API key override"""
    # Don't override the API key dependency to test actual authentication
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="function")
def sample_webhook_config():
    """Return a sample webhook configuration"""
    return WebhookConfig(
        source_path="/test-webhook",
        destination_url="https://example.com/webhook",
        transform_query="SELECT * FROM {{payload}}",
        filter_query="field1 = 'value1'",
        owner="test-owner"
    )


@pytest.fixture(scope="function")
def sample_payload():
    """Return a sample webhook payload"""
    return {
        "field1": "value1",
        "field2": "value2",
        "nested": {
            "key1": "value1",
            "key2": 123
        },
        "items": [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"}
        ]
    }


@pytest.fixture(scope="function")
def registered_webhook(db_manager, sample_webhook_config):
    """Register a webhook and return its details"""
    return asyncio.run(db_manager.register_webhook(sample_webhook_config))


@pytest.fixture(scope="function")
def reference_table_data():
    """Return sample data for a reference table"""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Item 1", "Item 2", "Item 3"],
        "category": ["A", "B", "A"],
        "active": [True, True, False]
    })


@pytest.fixture(scope="function")
def sample_udf_code():
    """Return sample Python UDF code"""
    return """
def test_function(text: str) -> str:
    # Transform text to uppercase
    if not text:
        return None
    return text.upper()
"""