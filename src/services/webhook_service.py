"""
Webhook service for business logic.
"""

import logging
from typing import Dict, Any, List, Optional

from ..database import WebhookRepository
from ..models.webhook import WebhookConfig, WebhookResponse

logger = logging.getLogger(__name__)


class WebhookService:
    """Service for webhook business logic"""

    def __init__(self, webhook_repository: WebhookRepository):
        self.webhook_repository = webhook_repository

    async def register_webhook(self, config: WebhookConfig) -> WebhookResponse:
        """Register or update a webhook
        
        Args:
            config: Webhook configuration
            
        Returns:
            Webhook response
        """
        logger.info(f"Registering webhook for path {config.source_path}")
        
        # Create or update the webhook
        webhook_data = await self.webhook_repository.create_or_update_webhook(config)
        
        # Convert to response model
        return WebhookResponse(
            id=str(webhook_data["id"]),
            source_path=webhook_data["source_path"],
            destination_url=webhook_data["destination_url"],
            transform_query=webhook_data["transform_query"],
            filter_query=webhook_data["filter_query"],
            owner=webhook_data["owner"],
            created_at=webhook_data["created_at"].isoformat(),
            updated_at=webhook_data["updated_at"].isoformat()
        )

    async def get_webhook_by_path(self, source_path: str) -> Optional[Dict[str, Any]]:
        """Get webhook by source path
        
        Args:
            source_path: The source path to look up
            
        Returns:
            Webhook data or None if not found
        """
        return await self.webhook_repository.get_webhook_by_path(source_path)

    async def get_webhook_by_id(self, webhook_id: str) -> Optional[WebhookResponse]:
        """Get webhook by ID
        
        Args:
            webhook_id: The webhook ID
            
        Returns:
            Webhook response or None if not found
        """
        webhook_data = await self.webhook_repository.get_webhook_by_id(webhook_id)
        
        if not webhook_data:
            return None
            
        return WebhookResponse(
            id=str(webhook_data["id"]),
            source_path=webhook_data["source_path"],
            destination_url=webhook_data["destination_url"],
            transform_query=webhook_data["transform_query"],
            filter_query=webhook_data["filter_query"],
            owner=webhook_data["owner"],
            created_at=webhook_data["created_at"].isoformat() if webhook_data["created_at"] else None,
            updated_at=webhook_data["updated_at"].isoformat() if webhook_data["updated_at"] else None,
            active=not webhook_data["source_path"].startswith('/inactive_')
        )

    async def list_webhooks(self) -> List[WebhookResponse]:
        """List all webhooks
        
        Returns:
            List of webhook responses
        """
        webhooks = await self.webhook_repository.list_webhooks()
        
        webhook_responses = []
        for webhook in webhooks:
            webhook_responses.append(WebhookResponse(
                id=str(webhook["id"]),
                source_path=webhook["source_path"],
                destination_url=webhook["destination_url"],
                transform_query="",  # Not included in list view
                filter_query=None,   # Not included in list view
                owner=webhook["owner"],
                created_at=webhook["created_at"].isoformat() if webhook["created_at"] else None,
                updated_at=webhook["updated_at"].isoformat() if webhook["updated_at"] else None,
                active=not webhook["source_path"].startswith('/inactive_')
            ))
        
        return webhook_responses

    async def update_webhook(self, webhook_id: str, config: WebhookConfig) -> Optional[WebhookResponse]:
        """Update a webhook
        
        Args:
            webhook_id: The webhook ID
            config: Updated webhook configuration
            
        Returns:
            Updated webhook response or None if not found
        """
        # Check if webhook exists
        if not await self.webhook_repository.webhook_exists(webhook_id):
            return None
            
        # Update the webhook
        webhook_data = await self.webhook_repository.create_or_update_webhook(config)
        
        # Convert to response model
        return WebhookResponse(
            id=str(webhook_data["id"]),
            source_path=webhook_data["source_path"],
            destination_url=webhook_data["destination_url"],
            transform_query=webhook_data["transform_query"],
            filter_query=webhook_data["filter_query"],
            owner=webhook_data["owner"],
            created_at=webhook_data["created_at"].isoformat(),
            updated_at=webhook_data["updated_at"].isoformat()
        )

    async def toggle_webhook_status(self, webhook_id: str, active: bool) -> Optional[WebhookResponse]:
        """Toggle webhook active status
        
        Args:
            webhook_id: The webhook ID
            active: Whether to activate or deactivate
            
        Returns:
            Updated webhook response or None if not found
        """
        # Get current webhook
        webhook_data = await self.webhook_repository.get_webhook_by_id(webhook_id)
        if not webhook_data:
            return None
            
        current_path = webhook_data["source_path"]
        
        if active and current_path.startswith('/inactive_'):
            # Remove the /inactive_ prefix to make the webhook active
            new_path = current_path.replace('/inactive_', '/')
            await self.webhook_repository.update_webhook_path(webhook_id, new_path)
        elif not active and not current_path.startswith('/inactive_'):
            # Add the /inactive_ prefix to make the webhook inactive
            new_path = f"/inactive_{webhook_id}"
            await self.webhook_repository.update_webhook_path(webhook_id, new_path)
        
        # Get updated webhook
        return await self.get_webhook_by_id(webhook_id)

    async def delete_webhook(self, webhook_id: str) -> bool:
        """Delete a webhook
        
        Args:
            webhook_id: The webhook ID
            
        Returns:
            True if deleted, False if not found
        """
        return await self.webhook_repository.delete_webhook(webhook_id)

    async def webhook_exists(self, webhook_id: str) -> bool:
        """Check if webhook exists
        
        Args:
            webhook_id: The webhook ID
            
        Returns:
            True if webhook exists, False otherwise
        """
        return await self.webhook_repository.webhook_exists(webhook_id)