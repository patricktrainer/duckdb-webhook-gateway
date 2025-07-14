#!/usr/bin/env python3
"""
Tests for repository layer database operations
"""

import asyncio
import json
import pytest
import uuid
from datetime import datetime

from src.database import (
    DatabaseConnection,
    DatabaseMigrations,
    WebhookRepository,
    EventRepository,
    ReferenceRepository
)
from src.models.webhook import WebhookConfig


class TestWebhookRepository:
    """Test WebhookRepository database operations"""

    @pytest.mark.asyncio
    async def test_create_webhook(self, db_connection, webhook_repository, sample_webhook_config):
        """Test creating a new webhook"""
        # Create webhook
        result = await webhook_repository.create_or_update_webhook(sample_webhook_config)
        
        # Verify result structure
        assert "id" in result
        assert result["source_path"] == sample_webhook_config.source_path
        assert result["destination_url"] == str(sample_webhook_config.destination_url)
        assert result["transform_query"] == sample_webhook_config.transform_query
        assert result["filter_query"] == sample_webhook_config.filter_query
        assert result["owner"] == sample_webhook_config.owner
        assert "created_at" in result
        assert "updated_at" in result
        
        # Verify it was stored in database
        stored_webhook = await webhook_repository.get_webhook_by_path(sample_webhook_config.source_path)
        assert stored_webhook is not None
        assert str(stored_webhook["id"]) == result["id"]

    @pytest.mark.asyncio
    async def test_update_existing_webhook(self, db_connection, webhook_repository, sample_webhook_config):
        """Test updating an existing webhook"""
        # Create initial webhook
        result1 = await webhook_repository.create_or_update_webhook(sample_webhook_config)
        original_id = result1["id"]
        
        # Update the webhook with same source_path
        updated_config = WebhookConfig(
            source_path=sample_webhook_config.source_path,  # Same path
            destination_url="https://updated.example.com/webhook",  # Different URL
            transform_query="SELECT updated FROM {{payload}}",  # Different query
            filter_query="updated = 'true'",  # Different filter
            owner="updated-owner"  # Different owner
        )
        
        result2 = await webhook_repository.create_or_update_webhook(updated_config)
        
        # Should have same ID (update, not create)
        assert result2["id"] == original_id
        
        # Should have updated values
        assert result2["destination_url"] == str(updated_config.destination_url)
        assert result2["transform_query"] == updated_config.transform_query
        assert result2["filter_query"] == updated_config.filter_query
        assert result2["owner"] == updated_config.owner

    @pytest.mark.asyncio
    async def test_get_webhook_by_path(self, db_connection, webhook_repository, sample_webhook_config):
        """Test retrieving webhook by path"""
        # Create webhook
        created = await webhook_repository.create_or_update_webhook(sample_webhook_config)
        
        # Retrieve by path
        retrieved = await webhook_repository.get_webhook_by_path(sample_webhook_config.source_path)
        
        # Verify they match
        assert retrieved is not None
        assert str(retrieved["id"]) == created["id"]
        assert retrieved["source_path"] == created["source_path"]

    @pytest.mark.asyncio
    async def test_get_webhook_by_path_not_found(self, db_connection, webhook_repository):
        """Test retrieving non-existent webhook"""
        result = await webhook_repository.get_webhook_by_path("/non-existent")
        assert result is None

    @pytest.mark.asyncio
    async def test_list_webhooks(self, db_connection, webhook_repository):
        """Test listing webhooks"""
        # Create multiple webhooks
        configs = [
            WebhookConfig(
                source_path="/webhook1",
                destination_url="https://example.com/webhook1",
                transform_query="SELECT * FROM {{payload}}",
                owner="owner1"
            ),
            WebhookConfig(
                source_path="/webhook2",
                destination_url="https://example.com/webhook2",
                transform_query="SELECT * FROM {{payload}}",
                owner="owner2"
            )
        ]
        
        for config in configs:
            await webhook_repository.create_or_update_webhook(config)
        
        # List all webhooks
        webhooks = await webhook_repository.list_webhooks()
        
        # Verify count
        assert len(webhooks) >= 2  # At least the ones we created
        
        # Verify structure
        for webhook in webhooks:
            assert "id" in webhook
            assert "source_path" in webhook
            assert "destination_url" in webhook
            assert "owner" in webhook


class TestEventRepository:
    """Test EventRepository database operations"""

    @pytest.mark.asyncio
    async def test_log_raw_event(self, db_connection, event_repository, sample_payload):
        """Test logging a raw event"""
        source_path = "/test-webhook"
        
        # Log event
        event_id = await event_repository.log_raw_event(source_path, sample_payload)
        
        # Verify event_id is returned
        assert event_id is not None
        assert isinstance(event_id, str)
        
        # Verify event was stored
        events = await event_repository.get_raw_events(limit=1)
        assert len(events) >= 1
        
        # Find our event
        our_event = None
        for event in events:
            if event["id"] == event_id:
                our_event = event
                break
        
        assert our_event is not None
        assert our_event["source_path"] == source_path
        assert json.loads(our_event["payload"]) == sample_payload

    @pytest.mark.asyncio
    async def test_log_transformed_event(self, db_connection, event_repository, sample_payload):
        """Test logging a transformed event"""
        # First log a raw event
        raw_event_id = await event_repository.log_raw_event("/test", sample_payload)
        
        # Log transformed event
        webhook_id = str(uuid.uuid4())
        transformed_payload = {"transformed": "data"}
        destination_url = "https://example.com/webhook"
        
        transformed_event_id = await event_repository.log_transformed_event(
            raw_event_id=raw_event_id,
            webhook_id=webhook_id,
            transformed_payload=transformed_payload,
            destination_url=destination_url,
            success=True,
            response_code=200,
            response_body="Success"
        )
        
        # Verify transformed event_id is returned
        assert transformed_event_id is not None
        assert isinstance(transformed_event_id, str)
        
        # Verify event was stored
        events = await event_repository.get_transformed_events(limit=1)
        assert len(events) >= 1
        
        # Find our event
        our_event = None
        for event in events:
            if event["id"] == transformed_event_id:
                our_event = event
                break
        
        assert our_event is not None
        assert our_event["raw_event_id"] == raw_event_id
        assert our_event["webhook_id"] == webhook_id
        assert json.loads(our_event["transformed_payload"]) == transformed_payload
        assert our_event["destination_url"] == destination_url
        assert our_event["success"] is True
        assert our_event["response_code"] == 200

    @pytest.mark.asyncio
    async def test_get_events_with_limit(self, db_connection, event_repository, sample_payload):
        """Test getting events with limit"""
        # Create multiple events
        event_ids = []
        for i in range(5):
            event_id = await event_repository.log_raw_event(f"/test-{i}", sample_payload)
            event_ids.append(event_id)
        
        # Get limited results
        events = await event_repository.get_raw_events(limit=3)
        assert len(events) == 3
        
        # Get all results
        all_events = await event_repository.get_raw_events(limit=100)
        assert len(all_events) >= 5  # At least the ones we created


class TestReferenceRepository:
    """Test ReferenceRepository database operations"""

    @pytest.mark.asyncio
    async def test_create_reference_table(self, db_connection, reference_repository, reference_table_data):
        """Test creating a reference table"""
        webhook_id = str(uuid.uuid4())
        table_name = "test_users"
        description = "Test user data"
        
        # Create reference table
        table_id = await reference_repository.create_reference_table(
            webhook_id=webhook_id,
            table_name=table_name,
            description=description,
            data=reference_table_data
        )
        
        # Verify table_id is returned
        assert table_id is not None
        assert isinstance(table_id, str)
        
        # Verify metadata was stored
        tables = await reference_repository.list_reference_tables(webhook_id=webhook_id)
        assert len(tables) >= 1
        
        # Find our table
        our_table = None
        for table in tables:
            if table["id"] == table_id:
                our_table = table
                break
        
        assert our_table is not None
        assert our_table["webhook_id"] == webhook_id
        assert table_name in our_table["table_name"]  # Actual name is prefixed
        assert our_table["description"] == description

    @pytest.mark.asyncio
    async def test_list_reference_tables(self, db_connection, reference_repository, reference_table_data):
        """Test listing reference tables"""
        webhook_id = str(uuid.uuid4())
        
        # Create multiple tables
        table_names = ["users", "products", "categories"]
        for name in table_names:
            await reference_repository.create_reference_table(
                webhook_id=webhook_id,
                table_name=name,
                description=f"Test {name} data",
                data=reference_table_data
            )
        
        # List tables for webhook
        tables = await reference_repository.list_reference_tables(webhook_id=webhook_id)
        assert len(tables) == len(table_names)
        
        # Verify structure
        for table in tables:
            assert "id" in table
            assert "webhook_id" in table
            assert "table_name" in table
            assert "description" in table
            assert table["webhook_id"] == webhook_id

    @pytest.mark.asyncio
    async def test_list_all_reference_tables(self, db_connection, reference_repository, reference_table_data):
        """Test listing all reference tables"""
        # Create tables for different webhooks
        webhook_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        
        for webhook_id in webhook_ids:
            await reference_repository.create_reference_table(
                webhook_id=webhook_id,
                table_name="test_table",
                description="Test data",
                data=reference_table_data
            )
        
        # List all tables
        all_tables = await reference_repository.list_reference_tables()
        assert len(all_tables) >= 2  # At least the ones we created
        
        # Verify we have tables from different webhooks
        webhook_ids_in_results = {table["webhook_id"] for table in all_tables}
        assert len(webhook_ids_in_results.intersection(set(webhook_ids))) >= 2


class TestDatabaseIntegration:
    """Test database integration and migrations"""

    @pytest.mark.asyncio
    async def test_database_initialization(self, test_db_path):
        """Test database initialization creates all tables"""
        # Create new connection and initialize
        db_connection = DatabaseConnection(test_db_path)
        migrations = DatabaseMigrations(db_connection)
        await migrations.initialize_schema()
        
        # Verify all tables exist
        tables = await db_connection.execute_query("SHOW TABLES")
        table_names = [table[0] for table in tables]
        
        expected_tables = {
            "webhooks",
            "raw_events", 
            "transformed_events",
            "reference_tables",
            "python_udfs"
        }
        
        for table in expected_tables:
            assert table in table_names
        
        # Clean up
        db_connection.close()

    @pytest.mark.asyncio
    async def test_foreign_key_constraints(self, db_connection, webhook_repository, event_repository, sample_webhook_config, sample_payload):
        """Test foreign key constraints work correctly"""
        # Create webhook
        webhook = await webhook_repository.create_or_update_webhook(sample_webhook_config)
        webhook_id = webhook["id"]
        
        # Create raw event
        raw_event_id = await event_repository.log_raw_event("/test", sample_payload)
        
        # Create transformed event with valid foreign keys
        transformed_event_id = await event_repository.log_transformed_event(
            raw_event_id=raw_event_id,
            webhook_id=webhook_id,
            transformed_payload={"result": "success"},
            destination_url="https://example.com",
            success=True,
            response_code=200,
            response_body="OK"
        )
        
        # Should succeed
        assert transformed_event_id is not None
        
        # Verify the relationships exist
        transformed_events = await event_repository.get_transformed_events(limit=1)
        assert len(transformed_events) >= 1
        
        event = transformed_events[0]
        assert event["raw_event_id"] == raw_event_id
        assert event["webhook_id"] == webhook_id