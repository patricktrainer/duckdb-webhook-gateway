"""
API routes for the webhook gateway.
"""

from . import (
    webhook_routes,
    query_routes,
    event_routes,
    reference_routes,
    udf_routes,
    stats_routes
)

__all__ = [
    "webhook_routes",
    "query_routes",
    "event_routes",
    "reference_routes",
    "udf_routes",
    "stats_routes",
]