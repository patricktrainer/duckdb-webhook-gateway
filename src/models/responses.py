"""
Common response models.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class BaseResponse(BaseModel):
    """Base response model"""
    status: str
    message: Optional[str] = None


class QueryResponse(BaseResponse):
    """Response model for query operations"""
    result: List[List[Any]]


class WebhookAcceptedResponse(BaseResponse):
    """Response model for webhook acceptance"""
    event_id: str


class StatsResponse(BaseResponse):
    """Response model for statistics"""
    webhook_count: int
    raw_event_count: int
    transformed_event_count: int
    webhook_success_rates: List[Dict[str, Any]]


class EchoResponse(BaseResponse):
    """Response model for echo webhook"""
    received_at: str
    payload: Dict[str, Any]