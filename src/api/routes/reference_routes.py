"""
Reference table routes.
"""

import logging
from io import StringIO
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from ...core.auth import get_api_key
from ...models.reference import ReferenceTableResponse
from ...services import UDFService
from ..dependencies import get_udf_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["reference"])


@router.post("/upload_table", response_model=dict)
async def upload_table(
    webhook_id: str = Form(...),
    table_name: str = Form(...),
    description: str = Form(...),
    file: UploadFile = File(...),
    udf_service: UDFService = Depends(get_udf_service),
    api_key: str = Depends(get_api_key)
):
    """Upload a reference table for a webhook"""
    try:
        # Read the file content
        content = await file.read()
        content_str = content.decode('utf-8')
        
        # Determine the file type and parse accordingly
        if file.filename and file.filename.endswith('.csv'):
            df = pd.read_csv(StringIO(content_str))
        elif file.filename and file.filename.endswith('.json'):
            df = pd.read_json(StringIO(content_str))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type. Please upload CSV or JSON.")
        
        # Upload the table
        table_id = await udf_service.upload_reference_table(webhook_id, table_name, description, df)
        
        return {"status": "success", "table_id": table_id, "table_name": table_name}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reference_tables", response_model=dict)
async def list_reference_tables(
    webhook_id: Optional[str] = None,
    udf_service: UDFService = Depends(get_udf_service),
    api_key: str = Depends(get_api_key)
):
    """List reference tables"""
    try:
        tables = await udf_service.list_reference_tables(webhook_id)
        return {"status": "success", "reference_tables": tables}
    except Exception as e:
        logger.error(f"Error listing reference tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))