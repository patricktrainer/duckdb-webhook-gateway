#!/usr/bin/env python3
"""
Tests for webhook registration and retrieval functionality
"""

import asyncio
import pytest

from src.app import WebhookConfig
from .test_helper import ensure_str


class TestWebhookRegistration:
    """Test webhook registration functionality"""

    @pytest.mark.asyncio
    async def test_register_new_webhook(self, db_manager, sample_webhook_config):
        """Test registering a new webhook"""
        # Register a new webhook
        result = await db_manager.register_webhook(sample_webhook_config)
        
        # Check the returned data
        assert result["source_path"] == sample_webhook_config.source_path
        assert result["destination_url"] == str(sample_webhook_config.destination_url)
        assert result["transform_query"] == sample_webhook_config.transform_query
        assert result["filter_query"] == sample_webhook_config.filter_query
        assert result["owner"] == sample_webhook_config.owner
        assert "id" in result
        
        # Verify the webhook was stored in the database
        webhook_db = await db_manager.execute_query(
            "SELECT id, source_path, destination_url, transform_query, filter_query, owner FROM webhooks WHERE id = ?",
            {"id": result["id"]}
        )
        
        assert ensure_str(webhook_db[0][0]) == ensure_str(result["id"])
        assert webhook_db[0][1] == sample_webhook_config.source_path
        assert webhook_db[0][2] == str(sample_webhook_config.destination_url)
        assert webhook_db[0][3] == sample_webhook_config.transform_query
        assert webhook_db[0][4] == sample_webhook_config.filter_query
        assert webhook_db[0][5] == sample_webhook_config.owner

    @pytest.mark.asyncio
    async def test_update_existing_webhook(self, db_manager, sample_webhook_config):
        """Test updating an existing webhook"""
        # Register a webhook initially
        initial_result = await db_manager.register_webhook(sample_webhook_config)
        initial_id = initial_result["id"]
        
        # Create an updated config with the same source_path
        updated_config = WebhookConfig(
            source_path=sample_webhook_config.source_path,
            destination_url="https://updated-example.com/webhook",
            transform_query="SELECT updated FROM {{payload}}",
            filter_query="field1 = 'updated'",
            owner="updated-owner"
        )
        
        # Update the webhook
        update_result = await db_manager.register_webhook(updated_config)
        
        # Verify the ID remains the same
        assert ensure_str(update_result["id"]) == ensure_str(initial_id)
        
        # Check the updated data
        assert update_result["destination_url"] == str(updated_config.destination_url)
        assert update_result["transform_query"] == updated_config.transform_query
        assert update_result["filter_query"] == updated_config.filter_query
        assert update_result["owner"] == updated_config.owner
        
        # Verify the webhook was updated in the database
        webhook_db = await db_manager.execute_query(
            "SELECT destination_url, transform_query, filter_query, owner FROM webhooks WHERE id = ?",
            {"id": initial_id}
        )
        
        assert webhook_db[0][0] == str(updated_config.destination_url)
        assert webhook_db[0][1] == updated_config.transform_query
        assert webhook_db[0][2] == updated_config.filter_query
        assert webhook_db[0][3] == updated_config.owner


class TestWebhookRetrieval:
    """Test webhook retrieval functionality"""

    @pytest.mark.asyncio
    async def test_get_webhook_by_path_existing(self, db_manager, registered_webhook):
        """Test retrieving an existing webhook by path"""
        # Retrieve the webhook by path
        webhook = await db_manager.get_webhook_by_path(registered_webhook["source_path"])
        
        # Check the retrieved data
        assert webhook is not None
        assert ensure_str(webhook["id"]) == ensure_str(registered_webhook["id"])
        assert webhook["source_path"] == registered_webhook["source_path"]
        assert webhook["destination_url"] == registered_webhook["destination_url"]
        assert webhook["transform_query"] == registered_webhook["transform_query"]
        assert webhook["filter_query"] == registered_webhook["filter_query"]
        assert webhook["owner"] == registered_webhook["owner"]

    @pytest.mark.asyncio
    async def test_get_webhook_by_path_nonexistent(self, db_manager):
        """Test retrieving a non-existent webhook by path"""
        # Try to retrieve a non-existent webhook
        webhook = await db_manager.get_webhook_by_path("/nonexistent-path")
        
        # Check that None is returned
        assert webhook is None

    @pytest.mark.asyncio
    async def test_get_webhook_by_path_case_sensitive(self, db_manager, registered_webhook):
        """Test that path retrieval is case sensitive"""
        # Get the original path and create a differently-cased version
        original_path = registered_webhook["source_path"]
        different_case_path = original_path.swapcase()
        
        # Retrieve using the differently-cased path
        webhook = await db_manager.get_webhook_by_path(different_case_path)
        
        # Should return None as paths are case-sensitive
        assert webhook is None
        
        # Original path should still work
        original_webhook = await db_manager.get_webhook_by_path(original_path)
        assert original_webhook is not None
        assert ensure_str(original_webhook["id"]) == ensure_str(registered_webhook["id"])

    @pytest.mark.asyncio
    async def test_path_normalization(self, db_manager):
        """Test that source paths are normalized with leading slash"""
        # Create a webhook config without a leading slash
        config = WebhookConfig(
            source_path="test-no-slash",
            destination_url="https://example.com/webhook",
            transform_query="SELECT * FROM {{payload}}",
            filter_query=None,
            owner="test-owner"
        )
        
        # Register the webhook
        result = await db_manager.register_webhook(config)
        
        # Verify the source path was normalized with a leading slash
        assert result["source_path"] == "/test-no-slash"
        
        # Check that we can retrieve it with the normalized path
        webhook = await db_manager.get_webhook_by_path("/test-no-slash")
        assert webhook is not None
        assert ensure_str(webhook["id"]) == ensure_str(result["id"])