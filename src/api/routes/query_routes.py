"""
Query routes for ad-hoc SQL queries.
"""

import logging

from fastapi import APIRouter, Depends, Form, HTTPException

from ...core.auth import get_api_key
from ...models.responses import QueryResponse
from ...services import WebhookProcessingService
from ..dependencies import get_webhook_processing_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["query"])


@router.post("/query", response_model=QueryResponse)
async def execute_query(
    query: str = Form(...),
    webhook_processing_service: WebhookProcessingService = Depends(get_webhook_processing_service),
    api_key: str = Depends(get_api_key)
):
    """Execute an ad-hoc SQL query against the DuckDB database"""
    try:
        result = await webhook_processing_service.execute_ad_hoc_query(query)
        return QueryResponse(status="success", result=result)
    except ValueError as e:
        logger.error(f"Invalid query: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))