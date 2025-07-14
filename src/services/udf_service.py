"""
UDF service for managing user-defined functions.
"""

import logging
from typing import Dict, Any, List, Optional

import pandas as pd

from ..database import ReferenceRepository
from ..models.reference import UDFResponse, ReferenceTableResponse

logger = logging.getLogger(__name__)


class UDFService:
    """Service for managing user-defined functions and reference tables"""

    def __init__(self, reference_repository: ReferenceRepository):
        self.reference_repository = reference_repository

    async def register_udf(self, webhook_id: str, function_name: str, function_code: str) -> Dict[str, Any]:
        """Register a Python UDF

        Args:
            webhook_id: ID of the webhook configuration
            function_name: Name of the Python function
            function_code: Python code defining the function

        Returns:
            UDF registration result
        """
        logger.info(f"Registering UDF {function_name} for webhook {webhook_id}")

        udf_id = await self.reference_repository.register_python_udf(
            webhook_id=webhook_id,
            function_name=function_name,
            function_code=function_code
        )

        # Create the actual DuckDB function name that will be used in queries
        safe_webhook_id = webhook_id.replace('-', '_')
        duckdb_function_name = f"udf_{safe_webhook_id}_{function_name}"

        return {
            "udf_id": udf_id,
            "function_name": function_name,
            "duckdb_function_name": duckdb_function_name
        }

    async def load_webhook_udfs(self, webhook_id: str):
        """Load all UDFs for a webhook

        Args:
            webhook_id: ID of the webhook configuration
        """
        await self.reference_repository.load_webhook_udfs(webhook_id)

    async def list_udfs(self, webhook_id: Optional[str] = None) -> List[UDFResponse]:
        """List user-defined functions

        Args:
            webhook_id: Optional webhook ID to filter by

        Returns:
            List of UDF responses
        """
        udfs = await self.reference_repository.list_udfs(webhook_id)

        udf_responses = []
        for udf in udfs:
            udf_responses.append(UDFResponse(
                id=udf["id"],
                webhook_id=udf["webhook_id"],
                name=udf["name"],
                code=udf["code"],
                created_at=udf["created_at"].isoformat() if udf["created_at"] else None,
                updated_at=udf["updated_at"].isoformat() if udf["updated_at"] else None
            ))

        return udf_responses

    async def upload_reference_table(
        self,
        webhook_id: str,
        table_name: str,
        description: str,
        data: pd.DataFrame
    ) -> str:
        """Upload a reference table

        Args:
            webhook_id: ID of the webhook configuration
            table_name: Name of the reference table
            description: Description of the reference table
            data: DataFrame containing the table data

        Returns:
            Table ID
        """
        logger.info(f"Uploading reference table {table_name} for webhook {webhook_id}")

        return await self.reference_repository.upload_reference_table(
            webhook_id=webhook_id,
            table_name=table_name,
            description=description,
            data=data
        )

    async def list_reference_tables(self, webhook_id: Optional[str] = None) -> List[ReferenceTableResponse]:
        """List reference tables

        Args:
            webhook_id: Optional webhook ID to filter by

        Returns:
            List of reference table responses
        """
        tables = await self.reference_repository.list_reference_tables(webhook_id)

        table_responses = []
        for table in tables:
            table_responses.append(ReferenceTableResponse(
                id=table["id"],
                webhook_id=table["webhook_id"],
                table_name=table["table_name"],
                description=table["description"],
                created_at=table["created_at"].isoformat() if table["created_at"] else None,
                updated_at=table["updated_at"].isoformat() if table["updated_at"] else None
            ))

        return table_responses

    async def cleanup_webhook_resources(self, webhook_id: str) -> Dict[str, int]:
        """Clean up all resources for a webhook

        Args:
            webhook_id: The webhook ID

        Returns:
            Dictionary with cleanup counts
        """
        logger.info(f"Cleaning up resources for webhook {webhook_id}")

        tables_deleted = await self.reference_repository.delete_reference_tables_for_webhook(webhook_id)
        udfs_deleted = await self.reference_repository.delete_udfs_for_webhook(webhook_id)

        return {
            "tables_deleted": tables_deleted,
            "udfs_deleted": udfs_deleted
        }