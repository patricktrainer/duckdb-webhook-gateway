"""
Database migrations and schema initialization.
"""

import logging
from .connection import DatabaseConnection

logger = logging.getLogger(__name__)


class DatabaseMigrations:
    """Handles database schema creation and migrations"""

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    async def initialize_schema(self):
        """Create the necessary tables if they don't exist"""
        logger.info("Initializing database schema")
        
        # Create webhooks table
        await self.db_connection.execute_query("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id UUID PRIMARY KEY,
                source_path VARCHAR UNIQUE,
                destination_url VARCHAR,
                transform_query VARCHAR,
                filter_query VARCHAR,
                owner VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
        # Create raw_events table
        await self.db_connection.execute_query("""
            CREATE TABLE IF NOT EXISTS raw_events (
                id UUID PRIMARY KEY,
                timestamp TIMESTAMP,
                source_path VARCHAR,
                payload JSON
            )
        """)
        
        # Create transformed_events table
        await self.db_connection.execute_query("""
            CREATE TABLE IF NOT EXISTS transformed_events (
                id UUID PRIMARY KEY,
                raw_event_id UUID,
                webhook_id UUID,
                timestamp TIMESTAMP,
                transformed_payload JSON,
                destination_url VARCHAR,
                success BOOLEAN,
                response_code INTEGER,
                response_body VARCHAR,
                FOREIGN KEY (raw_event_id) REFERENCES raw_events(id),
                FOREIGN KEY (webhook_id) REFERENCES webhooks(id)
            )
        """)
        
        # Create reference_tables metadata table
        await self.db_connection.execute_query("""
            CREATE TABLE IF NOT EXISTS reference_tables (
                id UUID PRIMARY KEY,
                webhook_id UUID,
                table_name VARCHAR,
                description VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (webhook_id) REFERENCES webhooks(id)
            )
        """)
        
        # Create python_udfs metadata table
        await self.db_connection.execute_query("""
            CREATE TABLE IF NOT EXISTS python_udfs (
                id UUID PRIMARY KEY,
                webhook_id UUID,
                function_name VARCHAR,
                function_code VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (webhook_id) REFERENCES webhooks(id)
            )
        """)
        
        logger.info("Database schema initialized")