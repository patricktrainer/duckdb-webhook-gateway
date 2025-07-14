"""
Webhook management routes.
"""

import logging
from typing import List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request

from ...core.auth import get_api_key
from ...models.webhook import WebhookConfig, WebhookResponse
from ...models.responses import BaseResponse, WebhookAcceptedResponse
from ...services import WebhookService, WebhookProcessingService
from ..dependencies import get_webhook_service, get_webhook_processing_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["webhooks"])


@router.post("/register", response_model=dict)
async def register_webhook(
    config: WebhookConfig,
    webhook_service: WebhookService = Depends(get_webhook_service),
    api_key: str = Depends(get_api_key)
):
    """Register or update a webhook configuration"""
    try:
        result = await webhook_service.register_webhook(config)
        return {"status": "success", "webhook": result}
    except Exception as e:
        logger.error(f"Error registering webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/webhooks", response_model=dict)
async def list_webhooks(
    webhook_service: WebhookService = Depends(get_webhook_service),
    api_key: str = Depends(get_api_key)
):
    """List all registered webhooks"""
    try:
        webhooks = await webhook_service.list_webhooks()
        return {"status": "success", "webhooks": webhooks}
    except Exception as e:
        logger.error(f"Error listing webhooks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/webhook/{webhook_id}", response_model=WebhookResponse)
async def get_webhook(
    webhook_id: str,
    webhook_service: WebhookService = Depends(get_webhook_service),
    api_key: str = Depends(get_api_key)
):
    """Get a specific webhook by ID"""
    try:
        webhook = await webhook_service.get_webhook_by_id(webhook_id)
        if not webhook:
            raise HTTPException(status_code=404, detail="Webhook not found")
        return webhook
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/webhook/{webhook_id}", response_model=WebhookResponse)
async def update_webhook(
    webhook_id: str,
    webhook_data: WebhookConfig,
    webhook_service: WebhookService = Depends(get_webhook_service),
    api_key: str = Depends(get_api_key)
):
    """Update a webhook"""
    try:
        webhook = await webhook_service.update_webhook(webhook_id, webhook_data)
        if not webhook:
            raise HTTPException(status_code=404, detail="Webhook not found")
        return webhook
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/webhook/{webhook_id}/status", response_model=WebhookResponse)
async def toggle_webhook_status(
    webhook_id: str,
    status_data: dict,
    webhook_service: WebhookService = Depends(get_webhook_service),
    api_key: str = Depends(get_api_key)
):
    """Toggle a webhook's active status"""
    try:
        active = status_data.get('active', True)
        webhook = await webhook_service.toggle_webhook_status(webhook_id, active)
        if not webhook:
            raise HTTPException(status_code=404, detail="Webhook not found")
        return webhook
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling webhook status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/webhooks/{webhook_id}", response_model=BaseResponse)
async def delete_webhook(
    webhook_id: str,
    webhook_service: WebhookService = Depends(get_webhook_service),
    api_key: str = Depends(get_api_key)
):
    """Delete a webhook"""
    try:
        success = await webhook_service.delete_webhook(webhook_id)
        if not success:
            raise HTTPException(status_code=404, detail="Webhook not found")
        return BaseResponse(status="success", message="Webhook deleted")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Webhook handling route - this should be in main.py to avoid router prefix issues