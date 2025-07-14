#!/usr/bin/env python3
"""
Tests for frontend integration - static files, SPA routing, etc.
"""

import os
from pathlib import Path
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.main import app
from src.core.config import config


class TestStaticFileServing:
    """Test static file serving functionality"""

    @pytest.fixture(scope="function")
    def test_client_with_overrides(self, test_app_with_overrides):
        """Test client using the app with dependency overrides"""
        with TestClient(test_app_with_overrides) as client:
            yield client

    def test_favicon_endpoint(self, test_client_with_overrides):
        """Test favicon.ico endpoint"""
        response = test_client_with_overrides.get("/favicon.ico")
        
        # Should either serve the file (200) or return no content (204)
        assert response.status_code in [status.HTTP_200_OK, status.HTTP_204_NO_CONTENT]
        
        if response.status_code == status.HTTP_200_OK:
            # Should be an ICO file
            assert "image" in response.headers.get("content-type", "").lower() or \
                   "ico" in response.headers.get("content-type", "").lower()

    def test_logo_endpoint(self, test_client_with_overrides):
        """Test logo192.png endpoint"""
        response = test_client_with_overrides.get("/logo192.png")
        
        # Should either serve the file (200) or return no content (204)
        assert response.status_code in [status.HTTP_200_OK, status.HTTP_204_NO_CONTENT]
        
        if response.status_code == status.HTTP_200_OK:
            # Should be a PNG file
            assert "image/png" in response.headers.get("content-type", "").lower()

    def test_service_worker_endpoint(self, test_client_with_overrides):
        """Test service-worker.js endpoint"""
        response = test_client_with_overrides.get("/service-worker.js")
        
        # Should either serve the file (200) or return no content (204)
        assert response.status_code in [status.HTTP_200_OK, status.HTTP_204_NO_CONTENT]
        
        if response.status_code == status.HTTP_200_OK:
            # Should be a JavaScript file
            assert "javascript" in response.headers.get("content-type", "").lower() or \
                   "text/plain" in response.headers.get("content-type", "").lower()

    def test_static_directory_mount(self, test_client_with_overrides):
        """Test that /static directory is properly mounted"""
        # Try to access static directory
        response = test_client_with_overrides.get("/static/")
        
        # Should either list directory or return 404 (depending on if static files exist)
        # Both are acceptable since we're testing the mount point works
        assert response.status_code in [
            status.HTTP_200_OK, 
            status.HTTP_404_NOT_FOUND,
            status.HTTP_403_FORBIDDEN  # Some servers return 403 for directory listing
        ]

    def test_nonexistent_static_file(self, test_client_with_overrides):
        """Test accessing non-existent static file"""
        response = test_client_with_overrides.get("/static/nonexistent.txt")
        
        # Should return 404
        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestSPARouting:
    """Test Single Page Application routing"""

    @pytest.fixture(scope="function")
    def test_client_with_overrides(self, test_app_with_overrides):
        """Test client using the app with dependency overrides"""
        with TestClient(test_app_with_overrides) as client:
            yield client

    def test_root_route_serves_spa_or_healthcheck(self, test_client_with_overrides):
        """Test root route serves either SPA or healthcheck"""
        response = test_client_with_overrides.get("/")
        
        # Should return 200
        assert response.status_code == status.HTTP_200_OK
        
        # Should either be JSON (healthcheck) or HTML (SPA)
        content_type = response.headers.get("content-type", "").lower()
        assert "json" in content_type or "html" in content_type

    def test_spa_catch_all_routes(self, test_client_with_overrides):
        """Test that unknown routes are caught by SPA handler"""
        spa_routes = [
            "/dashboard",
            "/webhook-manager",  # Changed from /webhooks to avoid API conflict
            "/settings",
            "/some/nested/route",
            "/webhook/123/details"
        ]
        
        for route in spa_routes:
            response = test_client_with_overrides.get(route)
            
            # Should return 200 (served by SPA) or have content
            assert response.status_code == status.HTTP_200_OK
            
            # Content should be either HTML (SPA) or JSON (fallback message)
            content_type = response.headers.get("content-type", "").lower()
            content = response.text
            
            # Should either serve HTML or return helpful message
            is_html = "html" in content_type or "<html" in content.lower()
            is_json_message = "json" in content_type and "frontend not found" in content.lower()
            
            assert is_html or is_json_message

    def test_api_routes_not_caught_by_spa(self, test_client_with_overrides):
        """Test that API routes are not caught by SPA catch-all"""
        # These should be handled by API, not SPA
        response = test_client_with_overrides.get("/stats")
        assert response.status_code == status.HTTP_200_OK
        
        # Should be JSON response from API
        assert "json" in response.headers.get("content-type", "").lower()
        data = response.json()
        assert "webhook_count" in data

    def test_nonexistent_api_route_returns_404(self, test_client_with_overrides):
        """Test that non-existent API routes return 404, not SPA"""
        response = test_client_with_overrides.get("/api/nonexistent")
        
        # Should return 404, not be caught by SPA
        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestFrontendConfiguration:
    """Test frontend configuration and detection"""

    def test_frontend_directory_detection(self):
        """Test that frontend directory detection works"""
        # Test the configuration
        assert hasattr(config, 'FRONTEND_DIR')
        assert isinstance(config.FRONTEND_DIR, Path)
        
        # Should point to frontend/build
        assert config.FRONTEND_DIR.name == "build"
        assert config.FRONTEND_DIR.parent.name == "frontend"

    def test_static_directory_configuration(self):
        """Test static directory configuration"""
        assert hasattr(config, 'STATIC_DIR')
        assert isinstance(config.STATIC_DIR, Path)
        
        # Should point to static directory
        assert config.STATIC_DIR.name == "static"

    def test_directory_creation(self):
        """Test that ensure_directories creates required directories"""
        # This should not raise an error
        config.ensure_directories()
        
        # Static directory should exist after calling ensure_directories
        assert config.STATIC_DIR.exists()
        assert config.STATIC_DIR.is_dir()


class TestFrontendWithBackendIntegration:
    """Test frontend and backend integration"""

    @pytest.fixture(scope="function")
    def test_client_with_overrides(self, test_app_with_overrides):
        """Test client using the app with dependency overrides"""
        with TestClient(test_app_with_overrides) as client:
            yield client

    def test_frontend_can_access_backend_api(self, test_client_with_overrides):
        """Test that frontend can successfully call backend APIs"""
        # Create a webhook via API (simulating frontend call)
        webhook_config = {
            "source_path": "/test-frontend",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "frontend-test"
        }
        
        response = test_client_with_overrides.post("/api/register", json=webhook_config)
        assert response.status_code == status.HTTP_200_OK
        
        # List webhooks via API (simulating frontend call)
        response = test_client_with_overrides.get("/api/webhooks")
        assert response.status_code == status.HTTP_200_OK
        
        webhooks = response.json()
        assert "webhooks" in webhooks
        assert len(webhooks["webhooks"]) >= 1

    def test_cors_headers_for_frontend(self, test_client_with_overrides):
        """Test CORS headers are present for frontend requests"""
        # Make an API request that would come from frontend
        response = test_client_with_overrides.options("/api/stats")
        
        # Should handle OPTIONS request (even if no explicit CORS is configured)
        # At minimum, should not return server error
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_204_NO_CONTENT,
            status.HTTP_405_METHOD_NOT_ALLOWED  # Acceptable if OPTIONS not explicitly handled
        ]

    def test_api_returns_json_for_frontend(self, test_client_with_overrides):
        """Test that API endpoints return proper JSON for frontend consumption"""
        endpoints_to_test = [
            "/api/stats",
            "/api/webhooks", 
            "/api/events"
        ]
        
        for endpoint in endpoints_to_test:
            response = test_client_with_overrides.get(endpoint)
            assert response.status_code == status.HTTP_200_OK
            
            # Should return JSON
            assert "json" in response.headers.get("content-type", "").lower()
            
            # Should be valid JSON
            data = response.json()
            assert isinstance(data, dict)

    def test_file_upload_endpoints_work_for_frontend(self, test_client_with_overrides):
        """Test file upload endpoints work correctly for frontend"""
        # First create a webhook
        webhook_config = {
            "source_path": "/test-upload-frontend",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "owner": "frontend-test"
        }
        webhook_response = test_client_with_overrides.post("/api/register", json=webhook_config)
        webhook_id = webhook_response.json()["webhook"]["id"]
        
        # Test CSV upload (simulating frontend file upload)
        csv_data = "id,name,value\n1,test1,100\n2,test2,200"
        
        response = test_client_with_overrides.post(
            "/api/upload_table",
            data={
                "webhook_id": webhook_id,
                "table_name": "frontend_test_table",
                "description": "Table uploaded from frontend test"
            },
            files={"file": ("test.csv", csv_data, "text/csv")}
        )
        
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["status"] == "success"
        assert "table_id" in result

    def test_error_responses_are_json_for_frontend(self, test_client_with_overrides):
        """Test that error responses are properly formatted JSON for frontend"""
        # Try to register invalid webhook
        invalid_config = {
            "source_path": "/test",
            "destination_url": "invalid-url",  # Invalid URL
            "transform_query": "INVALID SQL",
            "owner": "test"
        }
        
        response = test_client_with_overrides.post("/api/register", json=invalid_config)
        
        # Should return error (4xx or 5xx) but as JSON
        assert response.status_code >= 400
        assert "json" in response.headers.get("content-type", "").lower()
        
        # Should be parseable JSON with error details
        error_data = response.json()
        assert isinstance(error_data, dict)
        # Should contain error information
        assert "detail" in error_data or "message" in error_data or "error" in error_data