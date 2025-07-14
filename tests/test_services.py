#!/usr/bin/env python3
"""
Tests for service layer business logic
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.services.webhook_service import WebhookService
from src.services.event_service import EventService
from src.services.transformation_service import TransformationService
from src.services.filter_service import FilterService
from src.services.udf_service import UDFService
from src.services.webhook_processing_service import WebhookProcessingService
from src.models.webhook import WebhookConfig, WebhookResponse


class TestWebhookService:
    """Test WebhookService business logic"""

    @pytest.fixture
    def mock_webhook_repository(self):
        """Mock webhook repository"""
        return AsyncMock()

    @pytest.fixture
    def webhook_service(self, mock_webhook_repository):
        """WebhookService instance with mocked dependencies"""
        return WebhookService(mock_webhook_repository)

    @pytest.mark.asyncio
    async def test_register_webhook(self, webhook_service, mock_webhook_repository, sample_webhook_config):
        """Test webhook registration"""
        # Mock repository response - return a dict like the real repository does
        now = datetime.now()
        expected_response = {
            "id": "test-id",
            "source_path": "/test-webhook",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner",
            "created_at": now,
            "updated_at": now,
            "active": True
        }
        mock_webhook_repository.create_or_update_webhook.return_value = expected_response

        # Test registration
        result = await webhook_service.register_webhook(sample_webhook_config)

        # Verify repository was called correctly
        mock_webhook_repository.create_or_update_webhook.assert_called_once_with(sample_webhook_config)
        
        # Verify result contains expected data
        assert result.id == "test-id"
        assert result.source_path == sample_webhook_config.source_path
        assert isinstance(result, WebhookResponse)

    @pytest.mark.asyncio
    async def test_get_webhook_by_path(self, webhook_service, mock_webhook_repository):
        """Test retrieving webhook by path"""
        # Mock repository response
        now = datetime.now()
        expected_webhook = {
            "id": "test-id",
            "source_path": "/test-webhook",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": None,
            "owner": "test-owner",
            "created_at": now,
            "updated_at": now,
            "active": True
        }
        mock_webhook_repository.get_webhook_by_path.return_value = expected_webhook

        # Test retrieval
        result = await webhook_service.get_webhook_by_path("/test-webhook")

        # Verify repository was called correctly
        mock_webhook_repository.get_webhook_by_path.assert_called_once_with("/test-webhook")
        
        # Verify result
        assert result == expected_webhook

    @pytest.mark.asyncio
    async def test_get_webhook_by_path_not_found(self, webhook_service, mock_webhook_repository):
        """Test retrieving non-existent webhook"""
        # Mock repository response
        mock_webhook_repository.get_webhook_by_path.return_value = None

        # Test retrieval
        result = await webhook_service.get_webhook_by_path("/non-existent")

        # Verify result
        assert result is None


class TestEventService:
    """Test EventService business logic"""

    @pytest.fixture
    def mock_event_repository(self):
        """Mock event repository"""
        return AsyncMock()

    @pytest.fixture
    def event_service(self, mock_event_repository):
        """EventService instance with mocked dependencies"""
        return EventService(mock_event_repository)

    @pytest.mark.asyncio
    async def test_log_raw_event(self, event_service, mock_event_repository, sample_payload):
        """Test logging raw events"""
        # Mock repository response
        expected_event_id = "test-event-id"
        mock_event_repository.log_raw_event.return_value = expected_event_id

        # Test logging
        result = await event_service.log_raw_event("/test-path", sample_payload)

        # Verify repository was called correctly
        mock_event_repository.log_raw_event.assert_called_once_with("/test-path", sample_payload)
        
        # Verify result
        assert result == expected_event_id

    @pytest.mark.asyncio
    async def test_log_transformed_event(self, event_service, mock_event_repository):
        """Test logging transformed events"""
        # Test data
        raw_event_id = "raw-event-id"
        webhook_id = "webhook-id"
        transformed_payload = {"transformed": "data"}
        destination_url = "https://example.com/webhook"
        
        # Mock repository response
        expected_event_id = "transformed-event-id"
        mock_event_repository.log_transformed_event.return_value = expected_event_id

        # Test logging
        result = await event_service.log_transformed_event(
            raw_event_id=raw_event_id,
            webhook_id=webhook_id,
            transformed_payload=transformed_payload,
            destination_url=destination_url,
            success=True,
            response_code=200,
            response_body="Success"
        )

        # Verify repository was called correctly
        mock_event_repository.log_transformed_event.assert_called_once_with(
            raw_event_id=raw_event_id,
            webhook_id=webhook_id,
            transformed_payload=transformed_payload,
            destination_url=destination_url,
            success=True,
            response_code=200,
            response_body="Success"
        )
        
        # Verify result
        assert result == expected_event_id


class TestTransformationService:
    """Test TransformationService business logic"""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection"""
        mock = AsyncMock()
        # Mock the execute_transform method to return expected data
        mock.execute_transform = AsyncMock()
        return mock

    @pytest.fixture
    def transformation_service(self, mock_db_connection):
        """TransformationService instance with mocked dependencies"""
        return TransformationService(mock_db_connection)

    @pytest.mark.asyncio
    async def test_transform_payload(self, transformation_service, mock_db_connection, sample_payload):
        """Test payload transformation"""
        # Mock database response - return query result format
        expected_result = [("value1", "data")]  # Tuple format that DuckDB returns
        mock_db_connection.execute_query.return_value = expected_result
        
        # Mock the connection and query lock
        mock_db_connection.query_lock = AsyncMock()
        mock_db_connection.connection = MagicMock()
        
        # Test transformation
        result = await transformation_service.execute_transform(
            webhook_id="test-webhook-id",
            transform_query="SELECT field1, 'data' as transformed FROM {{payload}}",
            payload=sample_payload
        )

        # Verify database was called correctly
        mock_db_connection.execute_query.assert_called()
        
        # Verify result
        assert result == expected_result


class TestFilterService:
    """Test FilterService business logic"""

    @pytest.mark.asyncio
    async def test_apply_filter_no_query(self, sample_payload):
        """Test filter with no query (should pass)"""
        # Create a simple mock for db connection
        mock_db_connection = AsyncMock()
        filter_service = FilterService(mock_db_connection)
        
        # Test filter with no query
        result = await filter_service.apply_filter(
            webhook_id="test-webhook-id",
            filter_query=None,
            payload=sample_payload
        )

        # Verify result (no filter means it passes)
        assert result is True

    @pytest.mark.asyncio
    async def test_apply_filter_empty_query(self, sample_payload):
        """Test filter with empty query (should pass)"""
        # Create a simple mock for db connection
        mock_db_connection = AsyncMock()
        filter_service = FilterService(mock_db_connection)
        
        # Test filter with empty query
        result = await filter_service.apply_filter(
            webhook_id="test-webhook-id",
            filter_query="",
            payload=sample_payload
        )

        # Verify result (empty filter means it passes)
        assert result is True


class TestWebhookProcessingService:
    """Test WebhookProcessingService orchestration logic"""

    @pytest.fixture
    def mock_event_service(self):
        """Mock event service"""
        return AsyncMock()

    @pytest.fixture
    def mock_filter_service(self):
        """Mock filter service"""
        return AsyncMock()

    @pytest.fixture
    def mock_transformation_service(self):
        """Mock transformation service"""
        return AsyncMock()

    @pytest.fixture
    def mock_udf_service(self):
        """Mock UDF service"""
        return AsyncMock()

    @pytest.fixture
    def webhook_processing_service(self, mock_event_service, mock_filter_service, 
                                   mock_transformation_service, mock_udf_service):
        """WebhookProcessingService instance with mocked dependencies"""
        return WebhookProcessingService(
            mock_event_service,
            mock_filter_service,
            mock_transformation_service,
            mock_udf_service
        )

    @pytest.mark.asyncio
    async def test_process_webhook_success(self, webhook_processing_service, 
                                           mock_event_service, mock_filter_service,
                                           mock_transformation_service, mock_udf_service,
                                           sample_payload):
        """Test successful webhook processing"""
        # Mock webhook data
        webhook = {
            "id": "webhook-id",
            "source_path": "/test-webhook",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'value1'",
            "owner": "test-owner"
        }
        raw_event_id = "raw-event-id"
        
        # Mock service responses
        mock_filter_service.apply_filter.return_value = True
        mock_transformation_service.execute_transform.return_value = {"transformed": "data"}
        mock_event_service.log_transformed_event.return_value = "transformed-event-id"
        mock_event_service.ensure_raw_event_exists.return_value = None

        # Test processing
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "Success"
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response

            await webhook_processing_service.process_webhook(webhook, raw_event_id, sample_payload)

        # Verify services were called correctly
        mock_udf_service.load_webhook_udfs.assert_called_once_with("webhook-id")
        mock_filter_service.apply_filter.assert_called_once_with(
            "webhook-id", "field1 = 'value1'", sample_payload
        )
        mock_transformation_service.execute_transform.assert_called_once_with(
            "webhook-id", "SELECT * FROM {{payload}}", sample_payload
        )
        mock_event_service.log_transformed_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_webhook_filtered_out(self, webhook_processing_service,
                                                mock_event_service, mock_filter_service,
                                                mock_transformation_service, mock_udf_service,
                                                sample_payload):
        """Test webhook processing when filtered out"""
        # Mock webhook data
        webhook = {
            "id": "webhook-id",
            "source_path": "/test-webhook",
            "destination_url": "https://example.com/webhook",
            "transform_query": "SELECT * FROM {{payload}}",
            "filter_query": "field1 = 'different_value'",
            "owner": "test-owner"
        }
        raw_event_id = "raw-event-id"
        
        # Mock service responses (filter fails)
        mock_filter_service.apply_filter.return_value = False
        mock_event_service.log_transformed_event.return_value = "transformed-event-id"

        # Test processing
        await webhook_processing_service.process_webhook(webhook, raw_event_id, sample_payload)

        # Verify filter was checked
        mock_filter_service.apply_filter.assert_called_once_with(
            "webhook-id", "field1 = 'different_value'", sample_payload
        )
        
        # Verify transformation was NOT called
        mock_transformation_service.transform_payload.assert_not_called()
        
        # Verify event was logged as filtered out
        mock_event_service.log_transformed_event.assert_called_once()
        call_args = mock_event_service.log_transformed_event.call_args
        assert call_args[1]['success'] is False
        assert "filtered out" in call_args[1]['response_body'].lower()