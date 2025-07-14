"""
Custom exceptions for the webhook gateway.
"""


class WebhookGatewayException(Exception):
    """Base exception for webhook gateway"""
    pass


class WebhookNotFoundError(WebhookGatewayException):
    """Raised when a webhook is not found"""
    pass


class EventNotFoundError(WebhookGatewayException):
    """Raised when an event is not found"""
    pass


class TransformationError(WebhookGatewayException):
    """Raised when transformation fails"""
    pass


class FilterError(WebhookGatewayException):
    """Raised when filtering fails"""
    pass


class UDFError(WebhookGatewayException):
    """Raised when UDF operations fail"""
    pass


class DatabaseError(WebhookGatewayException):
    """Raised when database operations fail"""
    pass