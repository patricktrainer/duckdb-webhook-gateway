"""
Reference table and UDF-related data models.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class ReferenceTable(BaseModel):
    """Reference table model"""
    id: str
    webhook_id: str
    table_name: str
    description: str
    created_at: datetime
    updated_at: datetime


class ReferenceTableResponse(BaseModel):
    """Response model for reference table operations"""
    id: str
    webhook_id: str
    table_name: str
    description: str
    created_at: str
    updated_at: str


class PythonUDF(BaseModel):
    """Python UDF model"""
    id: str
    webhook_id: str
    function_name: str
    function_code: str
    created_at: datetime
    updated_at: datetime


class UDFResponse(BaseModel):
    """Response model for UDF operations"""
    id: str
    webhook_id: str
    name: str
    code: str
    created_at: str
    updated_at: str