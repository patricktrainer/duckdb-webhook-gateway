#!/usr/bin/env python3
"""
Helper functions for tests
"""

def ensure_str(value):
    """Ensure that a value is a string, handling UUID objects.
    
    Args:
        value: The value to convert to a string
        
    Returns:
        String representation of the value
    """
    return str(value) if value is not None else None