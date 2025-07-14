"""
Event routes for retrieving event data.
"""

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from ...core.auth import get_api_key
from ...models.events import EventResponse
from ...services import EventService
from ..dependencies import get_event_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["events"])


@router.get("/events", response_model=dict)
async def get_recent_events(
    limit: int = 5,
    event_service: EventService = Depends(get_event_service),
    api_key: str = Depends(get_api_key)
):
    """Get recent events"""
    try:
        events = await event_service.get_recent_events(limit)
        return {"status": "success", "events": events}
    except Exception as e:
        logger.error(f"Error getting recent events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/event/{event_id}/transformed", response_model=EventResponse)
async def get_transformed_event(
    event_id: str,
    event_service: EventService = Depends(get_event_service),
    api_key: str = Depends(get_api_key)
):
    """Get transformed event data for a specific event"""
    try:
        event = await event_service.get_event_details(event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        return event
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting transformed event data: {e}")
        raise HTTPException(status_code=500, detail=str(e))