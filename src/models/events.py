"""
Event-related data models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class RawEvent(BaseModel):
    """Raw webhook event model"""
    id: str
    timestamp: datetime
    source_path: str
    payload: Dict[str, Any]


class TransformedEvent(BaseModel):
    """Transformed webhook event model"""
    id: str
    raw_event_id: str
    webhook_id: str
    timestamp: datetime
    transformed_payload: Dict[str, Any]
    destination_url: str
    success: bool
    response_code: Optional[int] = None
    response_body: Optional[str] = None


class EventResponse(BaseModel):
    """Response model for event operations"""
    id: str
    timestamp: str
    source_path: str
    success: Optional[bool] = None
    response_code: Optional[int] = None
    raw_payload: Optional[Dict[str, Any]] = None
    transformed: Optional[Dict[str, Any]] = None