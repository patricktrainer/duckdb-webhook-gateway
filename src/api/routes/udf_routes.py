"""
UDF routes for managing user-defined functions.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, HTTPException

from ...core.auth import get_api_key
from ...models.reference import UDFResponse
from ...services import UDFService
from ..dependencies import get_udf_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["udf"])


@router.post("/register_udf", response_model=dict)
async def register_udf(
    webhook_id: str = Form(...),
    function_name: str = Form(...),
    function_code: str = Form(...),
    udf_service: UDFService = Depends(get_udf_service),
    api_key: str = Depends(get_api_key)
):
    """Register a Python UDF for use in DuckDB queries"""
    try:
        result = await udf_service.register_udf(webhook_id, function_name, function_code)
        return {"status": "success", **result}
    except Exception as e:
        logger.error(f"Error registering UDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/udfs", response_model=dict)
async def list_udfs(
    webhook_id: Optional[str] = None,
    udf_service: UDFService = Depends(get_udf_service),
    api_key: str = Depends(get_api_key)
):
    """List user-defined functions"""
    try:
        udfs = await udf_service.list_udfs(webhook_id)
        return {"status": "success", "udfs": udfs}
    except Exception as e:
        logger.error(f"Error listing UDFs: {e}")
        raise HTTPException(status_code=500, detail=str(e))