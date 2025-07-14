"""
Statistics routes.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request

from ...core.auth import get_api_key
from ...models.responses import StatsResponse, EchoResponse
from ...services import EventService, WebhookService
from ..dependencies import get_event_service, get_webhook_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["stats"])


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    webhook_service: WebhookService = Depends(get_webhook_service),
    event_service: EventService = Depends(get_event_service),
    api_key: str = Depends(get_api_key)
):
    """Get statistics about webhooks"""
    try:
        # Get webhook count
        webhook_count = await webhook_service.webhook_repository.get_webhook_count()
        
        # Get event statistics
        event_stats = await event_service.get_event_stats()
        
        return StatsResponse(
            status="success",
            webhook_count=webhook_count,
            raw_event_count=event_stats["raw_event_count"],
            transformed_event_count=event_stats["transformed_event_count"],
            webhook_success_rates=event_stats["webhook_success_rates"]
        )
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/echo-webhook", response_model=EchoResponse)
async def echo_webhook(
    request: Request,
    api_key: str = Depends(get_api_key)
):
    """Echo webhook endpoint for testing"""
    try:
        # Get the payload
        payload = await request.json()

        # Return it with some additional information
        return EchoResponse(
            status="success",
            message="Echo webhook received your payload",
            received_at=datetime.now().isoformat(),
            payload=payload
        )
    except Exception as e:
        logger.error(f"Error in echo webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))