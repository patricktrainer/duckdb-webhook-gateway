"""
FastAPI dependencies for dependency injection.
"""

from fastapi import Depends

from ..core.auth import get_api_key
from ..core.config import config
from ..database import (
    DatabaseConnection,
    WebhookRepository,
    EventRepository,
    ReferenceRepository,
    DatabaseMigrations
)
from ..services import (
    WebhookService,
    EventService,
    TransformationService,
    FilterService,
    UDFService,
    WebhookProcessingService
)

# Global database connection
_db_connection = None


def get_db_connection() -> DatabaseConnection:
    """Get the database connection"""
    global _db_connection
    if _db_connection is None:
        _db_connection = DatabaseConnection(config.DUCKDB_PATH)
    return _db_connection


def get_webhook_repository(db_connection: DatabaseConnection = Depends(get_db_connection)) -> WebhookRepository:
    """Get the webhook repository"""
    return WebhookRepository(db_connection)


def get_event_repository(db_connection: DatabaseConnection = Depends(get_db_connection)) -> EventRepository:
    """Get the event repository"""
    return EventRepository(db_connection)


def get_reference_repository(db_connection: DatabaseConnection = Depends(get_db_connection)) -> ReferenceRepository:
    """Get the reference repository"""
    return ReferenceRepository(db_connection)


def get_webhook_service(webhook_repository: WebhookRepository = Depends(get_webhook_repository)) -> WebhookService:
    """Get the webhook service"""
    return WebhookService(webhook_repository)


def get_event_service(event_repository: EventRepository = Depends(get_event_repository)) -> EventService:
    """Get the event service"""
    return EventService(event_repository)


def get_transformation_service(db_connection: DatabaseConnection = Depends(get_db_connection)) -> TransformationService:
    """Get the transformation service"""
    return TransformationService(db_connection)


def get_filter_service(db_connection: DatabaseConnection = Depends(get_db_connection)) -> FilterService:
    """Get the filter service"""
    return FilterService(db_connection)


def get_udf_service(reference_repository: ReferenceRepository = Depends(get_reference_repository)) -> UDFService:
    """Get the UDF service"""
    return UDFService(reference_repository)


def get_webhook_processing_service(
    event_service: EventService = Depends(get_event_service),
    filter_service: FilterService = Depends(get_filter_service),
    transformation_service: TransformationService = Depends(get_transformation_service),
    udf_service: UDFService = Depends(get_udf_service)
) -> WebhookProcessingService:
    """Get the webhook processing service"""
    return WebhookProcessingService(event_service, filter_service, transformation_service, udf_service)


def get_database_migrations(db_connection: DatabaseConnection = Depends(get_db_connection)) -> DatabaseMigrations:
    """Get the database migrations"""
    return DatabaseMigrations(db_connection)


async def initialize_database():
    """Initialize the database schema"""
    db_connection = get_db_connection()
    migrations = DatabaseMigrations(db_connection)
    await migrations.initialize_schema()


# Export commonly used dependencies
__all__ = [
    "get_api_key",
    "get_db_connection",
    "get_webhook_service",
    "get_event_service",
    "get_transformation_service",
    "get_filter_service",
    "get_udf_service",
    "get_webhook_processing_service",
    "initialize_database",
]