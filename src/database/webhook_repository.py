"""
Webhook repository for database operations.
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

from .base_repository import BaseRepository
from ..models.webhook import WebhookConfig

logger = logging.getLogger(__name__)


class WebhookRepository(BaseRepository):
    """Repository for webhook-related database operations"""

    async def create_or_update_webhook(self, config: WebhookConfig) -> Dict[str, Any]:
        """Create a new webhook or update an existing one
        
        Args:
            config: Webhook configuration
            
        Returns:
            Created or updated webhook details
        """
        webhook_id = str(uuid.uuid4())
        now = datetime.now()
        
        # Check if the source_path already exists
        existing = await self.execute_query(
            "SELECT id FROM webhooks WHERE source_path = ?",
            {"source_path": config.source_path}
        )

        if existing:
            webhook_id = str(existing[0][0])
            query = """
                UPDATE webhooks 
                SET destination_url = ?, transform_query = ?, filter_query = ?, 
                    owner = ?, updated_at = ?
                WHERE id = ?
            """
            params = {
                "destination_url": str(config.destination_url),
                "transform_query": config.transform_query,
                "filter_query": config.filter_query,
                "owner": config.owner,
                "updated_at": now,
                "id": webhook_id
            }
            logger.info(f"Updating webhook {webhook_id} for path {config.source_path}")
        else:
            query = """
                INSERT INTO webhooks (id, source_path, destination_url, transform_query, 
                                    filter_query, owner, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = {
                "id": webhook_id,
                "source_path": config.source_path,
                "destination_url": str(config.destination_url),
                "transform_query": config.transform_query,
                "filter_query": config.filter_query,
                "owner": config.owner,
                "created_at": now,
                "updated_at": now
            }
            logger.info(f"Creating new webhook {webhook_id} for path {config.source_path}")
        
        await self.execute_query(query, params)
        
        return {
            "id": webhook_id,
            "source_path": config.source_path,
            "destination_url": str(config.destination_url),
            "transform_query": config.transform_query,
            "filter_query": config.filter_query,
            "owner": config.owner,
            "created_at": now,
            "updated_at": now
        }

    async def get_webhook_by_path(self, source_path: str) -> Optional[Dict[str, Any]]:
        """Get a webhook configuration by its source path
        
        Args:
            source_path: The HTTP path to match
            
        Returns:
            Webhook configuration or None if not found
        """
        result = await self.execute_query(
            """
            SELECT id, source_path, destination_url, transform_query, filter_query, owner
            FROM webhooks
            WHERE source_path = ?
            """,
            {"source_path": source_path}
        )
        
        if not result:
            return None
        
        row = result[0]
        return {
            "id": row[0],
            "source_path": row[1],
            "destination_url": row[2],
            "transform_query": row[3],
            "filter_query": row[4],
            "owner": row[5]
        }

    async def get_webhook_by_id(self, webhook_id: str) -> Optional[Dict[str, Any]]:
        """Get a webhook configuration by ID
        
        Args:
            webhook_id: The webhook ID
            
        Returns:
            Webhook configuration or None if not found
        """
        result = await self.execute_query(
            """
            SELECT id, source_path, destination_url, transform_query, filter_query, owner, created_at, updated_at
            FROM webhooks
            WHERE id = ?
            """,
            {"id": webhook_id}
        )
        
        if not result:
            return None
        
        row = result[0]
        return {
            "id": row[0],
            "source_path": row[1],
            "destination_url": row[2],
            "transform_query": row[3],
            "filter_query": row[4],
            "owner": row[5],
            "created_at": row[6],
            "updated_at": row[7]
        }

    async def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all registered webhooks
        
        Returns:
            List of webhook configurations
        """
        result = await self.execute_query("""
            SELECT id, source_path, destination_url, owner, created_at, updated_at
            FROM webhooks
            ORDER BY updated_at DESC
        """)
        
        webhooks = []
        for row in result:
            webhooks.append({
                "id": row[0],
                "source_path": row[1],
                "destination_url": row[2],
                "owner": row[3],
                "created_at": row[4],
                "updated_at": row[5]
            })
        
        return webhooks

    async def delete_webhook(self, webhook_id: str) -> bool:
        """Delete a webhook by ID
        
        Args:
            webhook_id: The webhook ID to delete
            
        Returns:
            True if deleted, False if not found
        """
        # Check if the webhook exists
        webhook = await self.get_webhook_by_id(webhook_id)
        if not webhook:
            return False
        
        # Delete the webhook
        await self.execute_query(
            "DELETE FROM webhooks WHERE id = ?",
            {"id": webhook_id}
        )
        
        logger.info(f"Deleted webhook {webhook_id}")
        return True

    async def update_webhook_path(self, webhook_id: str, new_path: str) -> bool:
        """Update a webhook's source path
        
        Args:
            webhook_id: The webhook ID
            new_path: The new source path
            
        Returns:
            True if updated, False if not found
        """
        # Check if the webhook exists
        webhook = await self.get_webhook_by_id(webhook_id)
        if not webhook:
            return False
        
        # Update the path
        await self.execute_query(
            "UPDATE webhooks SET source_path = ? WHERE id = ?",
            {"source_path": new_path, "id": webhook_id}
        )
        
        return True

    async def webhook_exists(self, webhook_id: str) -> bool:
        """Check if a webhook exists
        
        Args:
            webhook_id: The webhook ID to check
            
        Returns:
            True if webhook exists, False otherwise
        """
        result = await self.execute_query(
            "SELECT COUNT(*) FROM webhooks WHERE id = ?",
            {"id": webhook_id}
        )
        return result[0][0] > 0

    async def get_webhook_count(self) -> int:
        """Get the total number of webhooks
        
        Returns:
            Total webhook count
        """
        result = await self.execute_query("SELECT COUNT(*) FROM webhooks")
        return result[0][0]