"""
Event repository for database operations.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class EventRepository(BaseRepository):
    """Repository for event-related database operations"""

    async def log_raw_event(self, source_path: str, payload: Dict[str, Any]) -> str:
        """Log a raw webhook event

        Args:
            source_path: The HTTP path the event was received on
            payload: The raw JSON payload

        Returns:
            Event ID
        """
        event_id = str(uuid.uuid4())
        now = datetime.now()

        await self.execute_query(
            """
            INSERT INTO raw_events (id, timestamp, source_path, payload)
            VALUES (?, ?, ?, ?)
            """,
            {
                "id": event_id,
                "timestamp": now,
                "source_path": source_path,
                "payload": json.dumps(payload)
            }
        )

        logger.info(f"Logged raw event {event_id} for path {source_path}")
        return event_id

    async def log_transformed_event(
        self,
        raw_event_id: str,
        webhook_id: str,
        transformed_payload: Dict[str, Any],
        destination_url: str,
        success: bool,
        response_code: Optional[int] = None,
        response_body: Optional[str] = None
    ) -> str:
        """Log a transformed webhook event

        Args:
            raw_event_id: ID of the original raw event
            webhook_id: ID of the webhook configuration
            transformed_payload: The transformed payload
            destination_url: The URL the payload was sent to
            success: Whether the delivery was successful
            response_code: The HTTP response code
            response_body: The HTTP response body

        Returns:
            Event ID
        """
        event_id = str(uuid.uuid4())
        now = datetime.now()

        # Ensure all UUIDs are strings
        raw_event_id_str = str(raw_event_id)
        webhook_id_str = str(webhook_id)

        await self.execute_query(
            """
            INSERT INTO transformed_events (
                id, raw_event_id, webhook_id, timestamp, transformed_payload,
                destination_url, success, response_code, response_body
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            {
                "id": event_id,
                "raw_event_id": raw_event_id_str,
                "webhook_id": webhook_id_str,
                "timestamp": now,
                "transformed_payload": json.dumps(transformed_payload),
                "destination_url": destination_url,
                "success": success,
                "response_code": response_code,
                "response_body": response_body
            }
        )

        logger.info(f"Logged transformed event {event_id} for raw event {raw_event_id_str}")
        return event_id

    async def get_raw_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get a raw event by ID

        Args:
            event_id: The event ID

        Returns:
            Raw event data or None if not found
        """
        result = await self.execute_query(
            """
            SELECT id, timestamp, source_path, payload
            FROM raw_events
            WHERE id = ?
            """,
            {"id": event_id}
        )

        if not result:
            return None

        row = result[0]
        return {
            "id": str(row[0]),
            "timestamp": row[1],
            "source_path": row[2],
            "payload": json.loads(row[3]) if row[3] else None
        }

    async def get_transformed_event(self, raw_event_id: str) -> Optional[Dict[str, Any]]:
        """Get a transformed event by raw event ID

        Args:
            raw_event_id: The raw event ID

        Returns:
            Transformed event data or None if not found
        """
        result = await self.execute_query(
            """
            SELECT id, webhook_id, timestamp, transformed_payload,
                   destination_url, success, response_code, response_body
            FROM transformed_events
            WHERE raw_event_id = ?
            """,
            {"raw_event_id": raw_event_id}
        )

        if not result:
            return None

        row = result[0]
        return {
            "id": str(row[0]),
            "webhook_id": str(row[1]),
            "timestamp": row[2],
            "transformed_payload": json.loads(row[3]) if row[3] else None,
            "destination_url": row[4],
            "success": bool(row[5]) if row[5] is not None else None,
            "response_code": row[6],
            "response_body": row[7]
        }

    async def get_recent_events(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recent events

        Args:
            limit: Number of events to return

        Returns:
            List of recent events
        """
        result = await self.execute_query(
            """
            SELECT r.id, r.timestamp, r.source_path, t.success, t.response_code
            FROM raw_events r
            LEFT JOIN transformed_events t ON r.id = t.raw_event_id
            ORDER BY r.timestamp DESC
            LIMIT ?
            """,
            {"limit": limit}
        )

        events = []
        for row in result:
            events.append({
                "id": str(row[0]),
                "timestamp": row[1],
                "source_path": row[2],
                "success": bool(row[3]) if row[3] is not None else None,
                "response_code": row[4]
            })

        return events

    async def raw_event_exists(self, event_id: str) -> bool:
        """Check if a raw event exists

        Args:
            event_id: The event ID to check

        Returns:
            True if event exists, False otherwise
        """
        result = await self.execute_query(
            "SELECT COUNT(*) FROM raw_events WHERE id = ?",
            {"id": event_id}
        )
        return result[0][0] > 0

    async def get_event_counts(self) -> Dict[str, int]:
        """Get event counts

        Returns:
            Dictionary with raw and transformed event counts
        """
        raw_count = await self.execute_query("SELECT COUNT(*) FROM raw_events")
        transformed_count = await self.execute_query("SELECT COUNT(*) FROM transformed_events")

        return {
            "raw_events": raw_count[0][0],
            "transformed_events": transformed_count[0][0]
        }

    async def get_webhook_success_rates(self) -> List[Dict[str, Any]]:
        """Get success rates for all webhooks

        Returns:
            List of webhook success rate data
        """
        result = await self.execute_query("""
            SELECT webhook_id, 
                   COUNT(*) as total_events,
                   SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as success_count,
                   CAST(SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as success_rate
            FROM transformed_events
            GROUP BY webhook_id
        """)

        success_rates = []
        for row in result:
            success_rates.append({
                "webhook_id": row[0],
                "total_events": row[1],
                "success_count": row[2],
                "success_rate": row[3]
            })

        return success_rates

    async def delete_events_for_webhook(self, webhook_id: str) -> int:
        """Delete all events for a webhook

        Args:
            webhook_id: The webhook ID

        Returns:
            Number of events deleted
        """
        # First delete transformed events
        transformed_result = await self.execute_query(
            "DELETE FROM transformed_events WHERE webhook_id = ?",
            {"webhook_id": webhook_id}
        )

        # Then delete raw events that no longer have transformed events
        raw_result = await self.execute_query("""
            DELETE FROM raw_events 
            WHERE id NOT IN (SELECT DISTINCT raw_event_id FROM transformed_events)
        """)

        return len(transformed_result) if transformed_result else 0