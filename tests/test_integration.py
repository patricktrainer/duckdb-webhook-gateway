#!/usr/bin/env python3
"""
Integration tests for end-to-end webhook processing
"""

import json
import os
import uuid
import time
from unittest.mock import patch, MagicMock

import pytest
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import threading

from src.app import app as webhook_gateway_app


# Test destination server to receive forwarded webhooks
class TestDestinationServer:
    """Simple server to receive and record webhooks"""
    
    def __init__(self, host="localhost", port=8001):
        """Initialize the test server"""
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.received_webhooks = []
        self.setup_routes()
        self.server_thread = None
    
    def setup_routes(self):
        """Set up the server routes"""
        @self.app.post("/webhook")
        async def receive_webhook(request: Request):
            """Receive a webhook and store it"""
            payload = await request.json()
            self.received_webhooks.append(payload)
            return JSONResponse({"status": "success"})
    
    def start(self):
        """Start the server in a separate thread"""
        def run_server():
            uvicorn.run(self.app, host=self.host, port=self.port, log_level="error")
        
        self.server_thread = threading.Thread(target=run_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        # Give the server time to start
        time.sleep(1)
    
    def stop(self):
        """Stop the server"""
        # This is a simple implementation; in a real scenario,
        # you would need to properly shut down the uvicorn server
        pass
    
    def clear_webhooks(self):
        """Clear stored webhooks"""
        self.received_webhooks = []


@pytest.fixture(scope="module")
def destination_server():
    """Start and stop the destination server for tests"""
    server = TestDestinationServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="function")
def test_client_with_env():
    """Return a FastAPI TestClient with API key environment variable set"""
    test_api_key = "test_api_key"
    os.environ["WEBHOOK_GATEWAY_API_KEY"] = test_api_key
    
    from fastapi.testclient import TestClient
    client = TestClient(webhook_gateway_app)
    
    return client, test_api_key


class TestEndToEndWebhookProcessing:
    """Test end-to-end webhook processing"""
    
    @pytest.mark.integration
    def test_simple_webhook_forwarding(self, test_client_with_env, destination_server):
        """Test end-to-end webhook processing with simple transformation"""
        client, api_key = test_client_with_env
        destination_server.clear_webhooks()
        
        # Register a webhook
        webhook_config = {
            "source_path": "/test-integration",
            "destination_url": f"http://{destination_server.host}:{destination_server.port}/webhook",
            "transform_query": "SELECT field1, field2, field1 || ' - ' || field2 AS combined FROM {{payload}}",
            "filter_query": None,
            "owner": "test-integration"
        }
        
        # Register the webhook
        register_response = client.post(
            "/register", 
            headers={"X-API-Key": api_key},
            json=webhook_config
        )
        
        assert register_response.status_code == 200
        webhook_id = register_response.json()["webhook"]["id"]
        
        # Send a webhook payload
        payload = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"  # This field should be excluded by transform
        }
        
        webhook_response = client.post(
            webhook_config["source_path"], 
            json=payload
        )
        
        assert webhook_response.status_code == 200
        event_id = webhook_response.json()["event_id"]
        
        # Give the async processing some time to complete
        time.sleep(1)
        
        # Check the webhook was processed and forwarded
        query_response = client.post(
            "/query", 
            headers={"X-API-Key": api_key},
            data={
                "query": f"""
                    SELECT success, response_code, transformed_payload
                    FROM transformed_events 
                    WHERE raw_event_id = '{event_id}'
                """
            }
        )
        
        assert query_response.status_code == 200
        query_result = query_response.json()["result"]
        assert len(query_result) == 1
        assert query_result[0][0] is True  # success
        assert query_result[0][1] == 200  # response_code
        
        # Verify the transformed payload has the expected structure
        transformed_payload = json.loads(query_result[0][2])
        assert "field1" in transformed_payload
        assert "field2" in transformed_payload
        assert "combined" in transformed_payload
        assert transformed_payload["field1"] == payload["field1"]
        assert transformed_payload["field2"] == payload["field2"]
        assert transformed_payload["combined"] == f"{payload['field1']} - {payload['field2']}"
        assert "field3" not in transformed_payload  # Should be excluded by transform
        
        # Verify the destination server received the webhook
        assert len(destination_server.received_webhooks) == 1
        received_webhook = destination_server.received_webhooks[0]
        assert received_webhook == transformed_payload
    
    @pytest.mark.integration
    def test_webhook_with_filtering(self, test_client_with_env, destination_server):
        """Test end-to-end webhook processing with filtering"""
        client, api_key = test_client_with_env
        destination_server.clear_webhooks()
        
        # Register a webhook with filter
        webhook_config = {
            "source_path": "/test-integration-filter",
            "destination_url": f"http://{destination_server.host}:{destination_server.port}/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "type = 'allowed'",
            "owner": "test-integration"
        }
        
        # Register the webhook
        register_response = client.post(
            "/register", 
            headers={"X-API-Key": api_key},
            json=webhook_config
        )
        
        assert register_response.status_code == 200
        
        # Send a webhook payload that should be filtered out
        filtered_payload = {
            "type": "blocked",
            "data": "test"
        }
        
        filtered_response = client.post(
            webhook_config["source_path"], 
            json=filtered_payload
        )
        
        assert filtered_response.status_code == 200
        filtered_event_id = filtered_response.json()["event_id"]
        
        # Send a webhook payload that should pass the filter
        passed_payload = {
            "type": "allowed",
            "data": "test"
        }
        
        passed_response = client.post(
            webhook_config["source_path"], 
            json=passed_payload
        )
        
        assert passed_response.status_code == 200
        passed_event_id = passed_response.json()["event_id"]
        
        # Give the async processing some time to complete
        time.sleep(1)
        
        # Check the filtered event was processed but not forwarded
        filtered_query_response = client.post(
            "/query", 
            headers={"X-API-Key": api_key},
            data={
                "query": f"""
                    SELECT success, response_body
                    FROM transformed_events 
                    WHERE raw_event_id = '{filtered_event_id}'
                """
            }
        )
        
        assert filtered_query_response.status_code == 200
        filtered_result = filtered_query_response.json()["result"]
        assert len(filtered_result) == 1
        assert filtered_result[0][0] is False  # success
        assert "Filtered out" in filtered_result[0][1]  # response_body
        
        # Check the passed event was processed and forwarded
        passed_query_response = client.post(
            "/query", 
            headers={"X-API-Key": api_key},
            data={
                "query": f"""
                    SELECT success, response_code
                    FROM transformed_events 
                    WHERE raw_event_id = '{passed_event_id}'
                """
            }
        )
        
        assert passed_query_response.status_code == 200
        passed_result = passed_query_response.json()["result"]
        assert len(passed_result) == 1
        assert passed_result[0][0] is True  # success
        assert passed_result[0][1] == 200  # response_code
        
        # Verify the destination server received only one webhook
        assert len(destination_server.received_webhooks) == 1
        received_webhook = destination_server.received_webhooks[0]
        assert received_webhook["type"] == "allowed"
    
    @pytest.mark.integration
    def test_webhook_with_reference_table(self, test_client_with_env, destination_server):
        """Test end-to-end webhook processing with reference table"""
        client, api_key = test_client_with_env
        destination_server.clear_webhooks()
        
        # Register a webhook
        webhook_config = {
            "source_path": "/test-integration-reftable",
            "destination_url": f"http://{destination_server.host}:{destination_server.port}/webhook",
            "transform_query": "SELECT * FROM {{payload}}",  # Will update this after creating ref table
            "filter_query": None,
            "owner": "test-integration"
        }
        
        # Register the webhook
        register_response = client.post(
            "/register", 
            headers={"X-API-Key": api_key},
            json=webhook_config
        )
        
        assert register_response.status_code == 200
        webhook_id = register_response.json()["webhook"]["id"]
        
        # Create a users CSV file
        import pandas as pd
        from io import StringIO
        
        users_csv = StringIO()
        users_df = pd.DataFrame({
            "user_id": [1, 2, 3],
            "username": ["john_doe", "jane_smith", "bob_jones"],
            "department": ["engineering", "product", "engineering"],
            "role": ["developer", "manager", "devops"]
        })
        users_df.to_csv(users_csv, index=False)
        users_csv.seek(0)
        
        # Upload the reference table
        import io
        csv_content = users_csv.getvalue().encode('utf-8')
        files = {
            'file': ('users.csv', io.BytesIO(csv_content), 'text/csv')
        }
        
        upload_response = client.post(
            "/upload_table",
            headers={"X-API-Key": api_key},
            data={
                "webhook_id": webhook_id,
                "table_name": "users",
                "description": "User information for enriching webhook data"
            },
            files=files
        )
        
        assert upload_response.status_code == 200
        table_name = upload_response.json()["table_name"]
        
        # Update the webhook with a transform query that uses the reference table
        updated_webhook_config = {
            "source_path": webhook_config["source_path"],
            "destination_url": webhook_config["destination_url"],
            "transform_query": f"""
                SELECT 
                    e.event_id, 
                    e.username, 
                    u.department, 
                    u.role 
                FROM {{{{payload}}}} e
                LEFT JOIN ref_{webhook_id.replace('-', '_')}_users u ON e.username = u.username
            """,
            "filter_query": None,
            "owner": webhook_config["owner"]
        }
        
        update_response = client.post(
            "/register", 
            headers={"X-API-Key": api_key},
            json=updated_webhook_config
        )
        
        assert update_response.status_code == 200
        
        # Send a webhook payload
        payload = {
            "event_id": "event123",
            "username": "jane_smith",
            "action": "login"
        }
        
        webhook_response = client.post(
            webhook_config["source_path"], 
            json=payload
        )
        
        assert webhook_response.status_code == 200
        event_id = webhook_response.json()["event_id"]
        
        # Give the async processing some time to complete
        time.sleep(1)
        
        # Verify the destination server received the webhook with enriched data
        assert len(destination_server.received_webhooks) == 1
        received_webhook = destination_server.received_webhooks[0]
        assert received_webhook["event_id"] == payload["event_id"]
        assert received_webhook["username"] == payload["username"]
        assert received_webhook["department"] == "product"  # From reference table
        assert received_webhook["role"] == "manager"  # From reference table
        
    @pytest.mark.integration
    def test_webhook_with_udf(self, test_client_with_env, destination_server):
        """Test end-to-end webhook processing with UDF"""
        client, api_key = test_client_with_env
        destination_server.clear_webhooks()
        
        # Register a webhook
        webhook_config = {
            "source_path": "/test-integration-udf",
            "destination_url": f"http://{destination_server.host}:{destination_server.port}/webhook",
            "transform_query": "SELECT * FROM {{payload}}",  # Will update this after creating UDF
            "filter_query": None,
            "owner": "test-integration"
        }
        
        # Register the webhook
        register_response = client.post(
            "/register", 
            headers={"X-API-Key": api_key},
            json=webhook_config
        )
        
        assert register_response.status_code == 200
        webhook_id = register_response.json()["webhook"]["id"]
        
        # Register a Python UDF
        udf_code = """
def extract_domain(email: str) -> str:
    # Extract domain from an email address
    if not email or '@' not in email:
        return None
    return email.split('@')[1]
"""
        
        udf_response = client.post(
            "/register_udf",
            headers={"X-API-Key": api_key},
            data={
                "webhook_id": webhook_id,
                "function_name": "extract_domain",
                "function_code": udf_code
            }
        )
        
        assert udf_response.status_code == 200
        udf_name = udf_response.json()["duckdb_function_name"]
        
        # Update the webhook with a transform query that uses the UDF
        updated_webhook_config = {
            "source_path": webhook_config["source_path"],
            "destination_url": webhook_config["destination_url"],
            "transform_query": f"""
                SELECT 
                    user_id,
                    email,
                    {udf_name}(email) AS domain
                FROM {{{{payload}}}}
            """,
            "filter_query": None,
            "owner": webhook_config["owner"]
        }
        
        update_response = client.post(
            "/register", 
            headers={"X-API-Key": api_key},
            json=updated_webhook_config
        )
        
        assert update_response.status_code == 200
        
        # Send a webhook payload
        payload = {
            "user_id": 123,
            "email": "user@example.com",
            "name": "Test User"
        }
        
        webhook_response = client.post(
            webhook_config["source_path"], 
            json=payload
        )
        
        assert webhook_response.status_code == 200
        event_id = webhook_response.json()["event_id"]
        
        # Give the async processing some time to complete
        time.sleep(1)
        
        # Verify the destination server received the webhook with the domain extracted
        assert len(destination_server.received_webhooks) == 1
        received_webhook = destination_server.received_webhooks[0]
        assert received_webhook["user_id"] == payload["user_id"]
        assert received_webhook["email"] == payload["email"]
        assert received_webhook["domain"] == "example.com"  # Extracted by UDF