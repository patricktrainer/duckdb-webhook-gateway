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

from src.main import app
from src.models.webhook import WebhookConfig
from src.database import DatabaseConnection, DatabaseMigrations
from src.database.webhook_repository import WebhookRepository
from src.database.event_repository import EventRepository
from src.database.reference_repository import ReferenceRepository
from src.services import (
    WebhookService,
    EventService,
    TransformationService,
    FilterService,
    UDFService,
    WebhookProcessingService
)
from src.core.auth import get_api_key
from src.api.dependencies import (
    get_db_connection,
    get_webhook_service,
    get_event_service,
    get_transformation_service,
    get_filter_service,
    get_udf_service,
    get_webhook_processing_service
)

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
def db_connection(test_db_path):
    """Create a test DatabaseConnection instance"""
    # Create a new DatabaseConnection with a test database
    connection = DatabaseConnection(db_path=test_db_path)

    # Initialize the database schema
    migrations = DatabaseMigrations(connection)
    asyncio.run(migrations.initialize_schema())

    # Return the connection for use in tests
    yield connection

    # Clean up - close connection and remove test database
    connection.close()
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
def registered_webhook(db_connection, sample_webhook_config):
    """Register a webhook and return its details"""
    from src.database import WebhookRepository
    webhook_repo = WebhookRepository(db_connection)
    return asyncio.run(webhook_repo.create_or_update_webhook(sample_webhook_config))


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


@pytest.fixture(scope="function")
def webhook_repository(db_connection):
    """Get a WebhookRepository instance"""
    return WebhookRepository(db_connection)


@pytest.fixture(scope="function")
def event_repository(db_connection):
    """Get an EventRepository instance"""
    return EventRepository(db_connection)


@pytest.fixture(scope="function")
def reference_repository(db_connection):
    """Get a ReferenceRepository instance"""
    return ReferenceRepository(db_connection)


@pytest.fixture(scope="function")
def webhook_service(webhook_repository):
    """Get a WebhookService instance"""
    return WebhookService(webhook_repository)


@pytest.fixture(scope="function")
def event_service(event_repository):
    """Get an EventService instance"""
    return EventService(event_repository)


@pytest.fixture(scope="function")
def transformation_service(db_connection):
    """Get a TransformationService instance"""
    return TransformationService(db_connection)


@pytest.fixture(scope="function")
def filter_service(db_connection):
    """Get a FilterService instance"""
    return FilterService(db_connection)


@pytest.fixture(scope="function")
def udf_service(reference_repository):
    """Get a UDFService instance"""
    return UDFService(reference_repository)


@pytest.fixture(scope="function")
def webhook_processing_service(event_service, filter_service, transformation_service, udf_service):
    """Get a WebhookProcessingService instance"""
    return WebhookProcessingService(event_service, filter_service, transformation_service, udf_service)


@pytest.fixture(scope="function")
def test_app_with_overrides(test_db_path):
    """FastAPI app with dependency overrides for testing"""
    # Create a test database connection
    test_db_connection = DatabaseConnection(db_path=test_db_path)
    
    # Initialize the test database schema
    migrations = DatabaseMigrations(test_db_connection)
    asyncio.run(migrations.initialize_schema())
    
    # Override dependencies to use test instances
    app.dependency_overrides[get_db_connection] = lambda: test_db_connection
    app.dependency_overrides[get_api_key] = lambda: TEST_API_KEY
    
    yield app
    
    # Clean up
    app.dependency_overrides.clear()
    test_db_connection.close()
    try:
        os.remove(test_db_path)
    except FileNotFoundError:
        pass