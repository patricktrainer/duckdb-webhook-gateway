"""
Base repository class for database operations.
"""

from abc import ABC
from typing import Any, Dict, Optional
from .connection import DatabaseConnection


class BaseRepository(ABC):
    """Base class for all repositories"""

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query through the database connection
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results
        """
        return await self.db_connection.execute_query(query, params)