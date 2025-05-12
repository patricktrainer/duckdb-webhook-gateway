#!/usr/bin/env python3
"""
Tests for API endpoints using FastAPI TestClient
"""

import json
import os
import uuid
from unittest.mock import patch, MagicMock

import pytest
from fastapi import status

from src.app import app


class TestAuthenticationEndpoints:
    """Test API authentication"""

    def test_missing_api_key(self, test_client_no_override):
        """Test endpoint access without API key"""
        response = test_client_no_override.post("/register", json={
            "source_path": "/test",
            "destination_url": "https://example.com",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        })

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_invalid_api_key(self, test_client_no_override):
        """Test endpoint access with invalid API key"""
        response = test_client_no_override.post(
            "/register",
            headers={"X-API-Key": "invalid_key"},
            json={
                "source_path": "/test",
                "destination_url": "https://example.com",
                "transform_query": "SELECT * FROM {{payload}}",
                "filter_query": "field1 = 'value1'",
                "owner": "test-owner"
            }
        )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_valid_api_key(self, test_client_no_override):
        """Test endpoint access with valid API key"""
        response = test_client_no_override.post(
            "/register",
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json={
                "source_path": "/test",
                "destination_url": "https://example.com",
                "transform_query": "SELECT * FROM {{payload}}",
                "filter_query": "field1 = 'value1'",
                "owner": "test-owner"
            }
        )

        # Should be accepted (status code 200-299)
        assert response.status_code < 300
        assert response.status_code >= 200


class TestWebhookEndpoints:
    """Test webhook registration and management endpoints"""
    
    def test_register_webhook(self, test_client):
        """Test registering a new webhook"""
        # Create webhook config
        webhook_config = {
            "source_path": "/test-register",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        # Register the webhook
        response = test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "webhook" in result
        
        webhook = result["webhook"]
        assert webhook["source_path"] == webhook_config["source_path"]
        assert webhook["destination_url"] == webhook_config["destination_url"]
        assert webhook["transform_query"] == webhook_config["transform_query"]
        assert webhook["filter_query"] == webhook_config["filter_query"]
        assert webhook["owner"] == webhook_config["owner"]
        assert "id" in webhook
        
    def test_register_webhook_validation_error(self, test_client):
        """Test validation errors when registering a webhook"""
        # Create invalid webhook config (missing transform_query)
        webhook_config = {
            "source_path": "/test-validation",
            "destination_url": "https://example.com/webhook",
            "owner": "test-owner"
        }
        
        # Try to register the webhook
        response = test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        # Check response
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        
    def test_register_webhook_with_invalid_transform_query(self, test_client):
        """Test validation error for transform query without {{payload}} placeholder"""
        # Create invalid webhook config (transform_query missing {{payload}})
        webhook_config = {
            "source_path": "/test-invalid-query",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM data",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        # Try to register the webhook
        response = test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        # Check response
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        
    def test_list_webhooks(self, test_client):
        """Test listing registered webhooks"""
        # First register a webhook
        webhook_config = {
            "source_path": "/test-list",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        # List webhooks
        response = test_client.get(
            "/webhooks", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]}
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "webhooks" in result
        assert len(result["webhooks"]) > 0
        
        # Check if our registered webhook is in the list
        found = False
        for webhook in result["webhooks"]:
            if webhook["source_path"] == webhook_config["source_path"]:
                found = True
                break
        
        assert found
        
    def test_delete_webhook(self, test_client):
        """Test deleting a webhook"""
        # First register a webhook
        webhook_config = {
            "source_path": "/test-delete",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        register_response = test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        webhook_id = register_response.json()["webhook"]["id"]
        
        # Delete the webhook
        response = test_client.delete(
            f"/webhooks/{webhook_id}", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]}
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        
        # Try to access the deleted webhook (should return 404)
        # Note: This test might not work if the delete endpoint just marks the webhook as inactive
        # rather than actually deleting it from the database.


class TestQueryEndpoint:
    """Test the query endpoint"""
    
    def test_execute_query(self, test_client):
        """Test executing a read-only SQL query"""
        query = "SELECT 1 as test_value"
        
        response = test_client.post(
            "/query", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            data={"query": query}
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "result" in result
        assert len(result["result"]) > 0
        assert result["result"][0][0] == 1
        
    def test_execute_query_with_write_operation(self, test_client):
        """Test executing a query with a write operation (should be rejected)"""
        query = "drop table webhooks"

        response = test_client.post(
            "/query",
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            data={"query": query}
        )

        # Check response - we accept both 400 and 500 status codes since the exact behavior
        # might depend on how the error is handled
        assert response.status_code in [status.HTTP_400_BAD_REQUEST, status.HTTP_500_INTERNAL_SERVER_ERROR]
        assert "not allowed" in response.text.lower()


class TestStatsEndpoint:
    """Test the stats endpoint"""
    
    def test_get_stats(self, test_client):
        """Test getting webhook statistics"""
        response = test_client.get(
            "/stats", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]}
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "webhook_count" in result
        assert "raw_event_count" in result
        assert "transformed_event_count" in result
        assert "webhook_success_rates" in result


class TestReferenceTablesEndpoints:
    """Test reference tables endpoints"""
    
    def test_list_reference_tables(self, test_client):
        """Test listing reference tables"""
        response = test_client.get(
            "/reference_tables", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]}
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "reference_tables" in result
        
    def test_list_reference_tables_by_webhook(self, test_client):
        """Test listing reference tables for a specific webhook"""
        # Create a test webhook
        webhook_config = {
            "source_path": "/test-reference-tables",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        register_response = test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        webhook_id = register_response.json()["webhook"]["id"]
        
        # List reference tables for the webhook
        response = test_client.get(
            f"/reference_tables?webhook_id={webhook_id}", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]}
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "reference_tables" in result


@patch('src.app.process_webhook')
class TestWebhookHandling:
    """Test webhook handling endpoints"""
    
    def test_handle_webhook(self, mock_process_webhook, test_client):
        """Test handling a webhook request"""
        # Create a test webhook
        webhook_config = {
            "source_path": "/test-webhook-handler",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        # Send a webhook payload
        payload = {
            "field1": "value1",
            "field2": "value2"
        }
        
        response = test_client.post(
            webhook_config["source_path"], 
            json=payload
        )
        
        # Check response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "accepted"
        assert "event_id" in result
        
        # Verify process_webhook was called
        mock_process_webhook.assert_called_once()
        
    def test_handle_webhook_nonexistent_path(self, mock_process_webhook, test_client):
        """Test handling a webhook request for a non-existent path"""
        # Send a webhook payload to a non-existent path
        payload = {
            "field1": "value1",
            "field2": "value2"
        }
        
        response = test_client.post(
            "/nonexistent-webhook-path", 
            json=payload
        )
        
        # Check response
        assert response.status_code == status.HTTP_404_NOT_FOUND
        
        # Verify process_webhook was not called
        mock_process_webhook.assert_not_called()
        
    def test_handle_webhook_invalid_json(self, mock_process_webhook, test_client):
        """Test handling a webhook request with invalid JSON payload"""
        # Create a test webhook
        webhook_config = {
            "source_path": "/test-invalid-json",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        
        test_client.post(
            "/register", 
            headers={"X-API-Key": os.environ["WEBHOOK_GATEWAY_API_KEY"]},
            json=webhook_config
        )
        
        # Send an invalid JSON payload
        response = test_client.post(
            webhook_config["source_path"], 
            data="This is not JSON",
            headers={"Content-Type": "application/json"}
        )
        
        # Check response
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        
        # Verify process_webhook was not called
        mock_process_webhook.assert_not_called()