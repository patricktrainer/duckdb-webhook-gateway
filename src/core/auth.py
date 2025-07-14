"""
Authentication utilities.
"""

import os
from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader

# Authentication
API_KEY_HEADER = APIKeyHeader(name="X-API-Key")


async def get_api_key(api_key: str = Depends(API_KEY_HEADER)):
    """Validate the API key

    Args:
        api_key: The API key from the request header

    Returns:
        The API key if valid

    Raises:
        HTTPException: If the API key is invalid
    """
    if api_key != os.environ.get("WEBHOOK_GATEWAY_API_KEY", "default_key"):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key