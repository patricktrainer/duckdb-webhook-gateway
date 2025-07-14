#!/usr/bin/env python3
"""
Tests for API versioning - both root and /api prefix routes
"""

import json
import os
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.main import app


class TestAPIVersioning:
    """Test that both root and /api prefix routes work correctly"""

    @pytest.fixture(scope="function")
    def test_client_with_overrides(self, test_app_with_overrides):
        """Test client using the app with dependency overrides"""
        with TestClient(test_app_with_overrides) as client:
            yield client

    def test_root_level_register_endpoint(self, test_client_with_overrides):
        """Test registering webhook via root-level endpoint"""
        webhook_config = {
            "source_path": "/test-root",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "test-owner"
        }
        
        response = test_client_with_overrides.post("/register", json=webhook_config)
        
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "webhook" in result

    def test_api_prefix_register_endpoint(self, test_client_with_overrides):
        """Test registering webhook via /api prefix endpoint"""
        webhook_config = {
            "source_path": "/test-api",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "test-owner"
        }
        
        response = test_client_with_overrides.post("/api/register", json=webhook_config)
        
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "webhook" in result

    def test_both_endpoints_create_same_webhook(self, test_client_with_overrides):
        """Test that both root and /api endpoints work identically"""
        webhook_config = {
            "source_path": "/test-same",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "test-owner"
        }
        
        # Register via root endpoint
        response1 = test_client_with_overrides.post("/register", json=webhook_config)
        assert response1.status_code == status.HTTP_200_OK
        webhook1 = response1.json()["webhook"]
        
        # Update via /api endpoint (should update, not create new)
        updated_config = webhook_config.copy()
        updated_config["destination_url"] = "https://updated.example.com/webhook"
        
        response2 = test_client_with_overrides.post("/api/register", json=updated_config)
        assert response2.status_code == status.HTTP_200_OK
        webhook2 = response2.json()["webhook"]
        
        # Should be same webhook (same ID) with updated URL
        assert webhook1["id"] == webhook2["id"]
        assert webhook2["destination_url"] == updated_config["destination_url"]

    def test_stats_endpoint_versioning(self, test_client_with_overrides):
        """Test stats endpoint on both root and /api prefix"""
        # Test root level
        response1 = test_client_with_overrides.get("/stats")
        assert response1.status_code == status.HTTP_200_OK
        stats1 = response1.json()
        
        # Test /api prefix
        response2 = test_client_with_overrides.get("/api/stats")
        assert response2.status_code == status.HTTP_200_OK
        stats2 = response2.json()
        
        # Should return identical data
        assert stats1 == stats2
        assert "webhook_count" in stats1
        assert "raw_event_count" in stats1

    def test_webhooks_list_endpoint_versioning(self, test_client_with_overrides):
        """Test webhooks list endpoint on both root and /api prefix"""
        # First create a webhook
        webhook_config = {
            "source_path": "/test-list",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "test-owner"
        }
        test_client_with_overrides.post("/register", json=webhook_config)
        
        # Test root level
        response1 = test_client_with_overrides.get("/webhooks")
        assert response1.status_code == status.HTTP_200_OK
        webhooks1 = response1.json()
        
        # Test /api prefix
        response2 = test_client_with_overrides.get("/api/webhooks")
        assert response2.status_code == status.HTTP_200_OK
        webhooks2 = response2.json()
        
        # Should return identical data
        assert webhooks1 == webhooks2
        assert "webhooks" in webhooks1
        assert len(webhooks1["webhooks"]) >= 1

    def test_events_endpoint_versioning(self, test_client_with_overrides):
        """Test events endpoint on both root and /api prefix"""
        # Test root level
        response1 = test_client_with_overrides.get("/events")
        assert response1.status_code == status.HTTP_200_OK
        events1 = response1.json()
        
        # Test /api prefix
        response2 = test_client_with_overrides.get("/api/events")
        assert response2.status_code == status.HTTP_200_OK
        events2 = response2.json()
        
        # Should return identical data
        assert events1 == events2
        assert "events" in events1

    def test_query_endpoint_versioning(self, test_client_with_overrides):
        """Test query endpoint on both root and /api prefix"""
        query = "SELECT COUNT(*) FROM webhooks"
        
        # Test root level
        response1 = test_client_with_overrides.post(
            "/query",
            data={"query": query}
        )
        assert response1.status_code == status.HTTP_200_OK
        result1 = response1.json()
        
        # Test /api prefix
        response2 = test_client_with_overrides.post(
            "/api/query",
            data={"query": query}
        )
        assert response2.status_code == status.HTTP_200_OK
        result2 = response2.json()
        
        # Should return identical data
        assert result1 == result2
        assert "result" in result1

    def test_upload_table_endpoint_versioning(self, test_client_with_overrides):
        """Test upload table endpoint on both root and /api prefix"""
        # First create a webhook
        webhook_config = {
            "source_path": "/test-upload",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "test-owner"
        }
        webhook_response = test_client_with_overrides.post("/register", json=webhook_config)
        webhook_id = webhook_response.json()["webhook"]["id"]
        
        # Create test CSV data
        csv_data = "id,name,value\n1,test1,100\n2,test2,200"
        
        # Test root level
        response1 = test_client_with_overrides.post(
            "/upload_table",
            data={
                "webhook_id": webhook_id,
                "table_name": "test_table_root",
                "description": "Test table via root endpoint"
            },
            files={"file": ("test.csv", csv_data, "text/csv")}
        )
        assert response1.status_code == status.HTTP_200_OK
        result1 = response1.json()
        
        # Test /api prefix
        response2 = test_client_with_overrides.post(
            "/api/upload_table",
            data={
                "webhook_id": webhook_id,
                "table_name": "test_table_api",
                "description": "Test table via API endpoint"
            },
            files={"file": ("test.csv", csv_data, "text/csv")}
        )
        assert response2.status_code == status.HTTP_200_OK
        result2 = response2.json()
        
        # Both should succeed
        assert result1["status"] == "success"
        assert result2["status"] == "success"
        assert "table_id" in result1
        assert "table_id" in result2

    def test_register_udf_endpoint_versioning(self, test_client_with_overrides):
        """Test register UDF endpoint on both root and /api prefix"""
        # First create a webhook
        webhook_config = {
            "source_path": "/test-udf",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "test-owner"
        }
        webhook_response = test_client_with_overrides.post("/register", json=webhook_config)
        webhook_id = webhook_response.json()["webhook"]["id"]
        
        udf_code = """
def test_function(text: str) -> str:
    return text.upper() if text else None
"""
        
        # Test root level
        response1 = test_client_with_overrides.post(
            "/register_udf",
            data={
                "webhook_id": webhook_id,
                "function_name": "test_function_root",
                "function_code": udf_code
            }
        )
        assert response1.status_code == status.HTTP_200_OK
        result1 = response1.json()
        
        # Test /api prefix
        response2 = test_client_with_overrides.post(
            "/api/register_udf",
            data={
                "webhook_id": webhook_id,
                "function_name": "test_function_api",
                "function_code": udf_code
            }
        )
        assert response2.status_code == status.HTTP_200_OK
        result2 = response2.json()
        
        # Both should succeed
        assert result1["status"] == "success"
        assert result2["status"] == "success"
        assert "udf_id" in result1
        assert "udf_id" in result2