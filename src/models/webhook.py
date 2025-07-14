"""
Webhook-related data models.
"""

from pydantic import BaseModel, HttpUrl, field_validator
from typing import Optional


class WebhookConfig(BaseModel):
    """Configuration for a webhook endpoint"""
    source_path: str
    destination_url: HttpUrl
    transform_query: str
    filter_query: Optional[str] = None
    owner: str

    @field_validator('source_path')
    def validate_source_path(cls, v):
        """Ensure the source path starts with a slash"""
        if not v.startswith('/'):
            return f"/{v}"
        return v

    @field_validator('transform_query')
    def validate_transform_query(cls, v):
        """Basic validation for the transform query"""
        if "{{payload}}" not in v:
            raise ValueError("Transform query must include {{payload}} placeholder")
        return v


class WebhookResponse(BaseModel):
    """Response model for webhook operations"""
    id: str
    source_path: str
    destination_url: str
    transform_query: str
    filter_query: Optional[str] = None
    owner: str
    created_at: str
    updated_at: str
    active: bool = True