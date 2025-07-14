"""
Configuration management.
"""

import os
from pathlib import Path


class Config:
    """Application configuration"""
    
    # Database
    DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "webhook_gateway.db")
    DUCKDB_MAX_WORKERS = int(os.environ.get("DUCKDB_MAX_WORKERS", "4"))
    
    # Authentication
    API_KEY = os.environ.get("WEBHOOK_GATEWAY_API_KEY", "default_key")
    
    # Paths
    STATIC_DIR = Path("static")
    FRONTEND_DIR = Path("frontend/build")
    
    # Server
    HOST = os.environ.get("HOST", "0.0.0.0")
    PORT = int(os.environ.get("PORT", "8000"))
    
    @classmethod
    def ensure_directories(cls):
        """Ensure required directories exist"""
        cls.STATIC_DIR.mkdir(exist_ok=True)
        
        # Ensure database directory exists
        if os.path.dirname(cls.DUCKDB_PATH):
            os.makedirs(os.path.dirname(cls.DUCKDB_PATH), exist_ok=True)


# Global config instance
config = Config()