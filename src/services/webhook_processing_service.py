"""
Webhook processing service that orchestrates the complete webhook processing flow.
"""

import logging
from typing import Dict, Any

from .event_service import EventService
from .filter_service import FilterService
from .transformation_service import TransformationService
from .udf_service import UDFService

logger = logging.getLogger(__name__)


class WebhookProcessingService:
    """Service that orchestrates the complete webhook processing flow"""

    def __init__(
        self,
        event_service: EventService,
        filter_service: FilterService,
        transformation_service: TransformationService,
        udf_service: UDFService
    ):
        self.event_service = event_service
        self.filter_service = filter_service
        self.transformation_service = transformation_service
        self.udf_service = udf_service

    async def process_webhook(self, webhook: Dict[str, Any], raw_event_id: str, payload: Dict[str, Any]):
        """Process a webhook event through the complete flow

        Args:
            webhook: Webhook configuration
            raw_event_id: ID of the raw event
            payload: The raw JSON payload
        """
        try:
            # Ensure the raw event exists in the database
            await self.event_service.ensure_raw_event_exists(
                raw_event_id, webhook["source_path"], payload
            )

            # Load any UDFs for this webhook
            await self.udf_service.load_webhook_udfs(webhook["id"])

            # Apply the filter query if it exists
            should_forward = True
            if webhook["filter_query"]:
                should_forward = await self.filter_service.apply_filter(
                    webhook["id"],
                    webhook["filter_query"],
                    payload
                )

            if not should_forward:
                logger.info(f"Event {raw_event_id} filtered out for webhook {webhook['id']}")
                await self.event_service.log_transformed_event(
                    raw_event_id=raw_event_id,
                    webhook_id=webhook["id"],
                    transformed_payload={},
                    destination_url=webhook["destination_url"],
                    success=False,
                    response_code=None,
                    response_body="Filtered out by filter_query"
                )
                return

            # Execute the transform query
            transformed_payload = await self.transformation_service.execute_transform(
                webhook["id"],
                webhook["transform_query"],
                payload
            )

            # Forward the transformed payload to the destination URL
            success, response_code, response_body = await self.event_service.forward_webhook(
                webhook["destination_url"],
                transformed_payload
            )

            # Log the transformed event
            try:
                await self.event_service.log_transformed_event(
                    raw_event_id=raw_event_id,
                    webhook_id=webhook["id"],
                    transformed_payload=transformed_payload,
                    destination_url=webhook["destination_url"],
                    success=success,
                    response_code=response_code,
                    response_body=response_body
                )
                logger.info(f"Processed webhook {webhook['id']} for event {raw_event_id}, success: {success}")
            except Exception as log_err:
                logger.error(f"Error logging transformed event: {log_err}")

        except Exception as e:
            logger.error(f"Error processing webhook: {e}")
            # Try to log the error, but don't raise if it fails
            try:
                await self.event_service.log_transformed_event(
                    raw_event_id=raw_event_id,
                    webhook_id=webhook["id"],
                    transformed_payload={},
                    destination_url=webhook["destination_url"],
                    success=False,
                    response_code=None,
                    response_body=f"Error: {str(e)}"
                )
            except Exception as log_err:
                logger.error(f"Error logging webhook error: {log_err}")

    async def execute_ad_hoc_query(self, query: str) -> list:
        """Execute an ad-hoc SQL query

        Args:
            query: SQL query to execute

        Returns:
            Query results
        """
        # Basic SQL injection prevention
        if any(keyword in query.upper() for keyword in ["DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE"]):
            raise ValueError("Write operations not allowed in ad-hoc queries")

        # Use the transformation service's database connection to execute the query
        result = await self.transformation_service.db_connection.execute_query(query)

        # Convert result to a more JSON-friendly format
        formatted_result = []
        for row in result:
            formatted_row = []
            for value in row:
                if hasattr(value, 'isoformat'):  # datetime objects
                    formatted_row.append(value.isoformat())
                else:
                    formatted_row.append(value)
            formatted_result.append(formatted_row)

        return formatted_result