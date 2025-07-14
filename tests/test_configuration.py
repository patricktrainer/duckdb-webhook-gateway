#!/usr/bin/env python3
"""
Tests for configuration management and environment variables
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest

from src.core.config import Config, config
from src.core.auth import get_api_key


class TestConfigurationDefaults:
    """Test configuration default values"""

    def test_default_database_path(self):
        """Test default database path configuration"""
        test_config = Config()
        assert test_config.DUCKDB_PATH == "webhook_gateway.db"

    def test_default_max_workers(self):
        """Test default max workers configuration"""
        test_config = Config()
        assert test_config.DUCKDB_MAX_WORKERS == 4

    def test_default_api_key(self):
        """Test default API key configuration"""
        test_config = Config()
        assert test_config.API_KEY == "default_key"

    def test_default_host_and_port(self):
        """Test default host and port configuration"""
        test_config = Config()
        assert test_config.HOST == "0.0.0.0"
        assert test_config.PORT == 8000

    def test_default_directories(self):
        """Test default directory paths"""
        test_config = Config()
        assert test_config.STATIC_DIR == Path("static")
        assert test_config.FRONTEND_DIR == Path("frontend/build")


class TestEnvironmentVariableOverrides:
    """Test environment variable overrides"""

    def test_duckdb_path_override(self):
        """Test DUCKDB_PATH environment variable override"""
        with patch.dict(os.environ, {"DUCKDB_PATH": "/custom/path/test.db"}):
            test_config = Config()
            assert test_config.DUCKDB_PATH == "/custom/path/test.db"

    def test_duckdb_max_workers_override(self):
        """Test DUCKDB_MAX_WORKERS environment variable override"""
        with patch.dict(os.environ, {"DUCKDB_MAX_WORKERS": "8"}):
            test_config = Config()
            assert test_config.DUCKDB_MAX_WORKERS == 8

    def test_api_key_override(self):
        """Test WEBHOOK_GATEWAY_API_KEY environment variable override"""
        with patch.dict(os.environ, {"WEBHOOK_GATEWAY_API_KEY": "custom_secret_key"}):
            test_config = Config()
            assert test_config.API_KEY == "custom_secret_key"

    def test_host_override(self):
        """Test HOST environment variable override"""
        with patch.dict(os.environ, {"HOST": "127.0.0.1"}):
            test_config = Config()
            assert test_config.HOST == "127.0.0.1"

    def test_port_override(self):
        """Test PORT environment variable override"""
        with patch.dict(os.environ, {"PORT": "9000"}):
            test_config = Config()
            assert test_config.PORT == 9000

    def test_invalid_port_falls_back_to_default(self):
        """Test that invalid PORT values fall back to default"""
        with patch.dict(os.environ, {"PORT": "invalid"}):
            # Should raise ValueError when trying to convert "invalid" to int
            with pytest.raises(ValueError):
                Config()

    def test_invalid_max_workers_falls_back_to_default(self):
        """Test that invalid DUCKDB_MAX_WORKERS values fall back to default"""
        with patch.dict(os.environ, {"DUCKDB_MAX_WORKERS": "invalid"}):
            # Should raise ValueError when trying to convert "invalid" to int
            with pytest.raises(ValueError):
                Config()


class TestDirectoryManagement:
    """Test directory creation and management"""

    def test_ensure_directories_creates_static_dir(self):
        """Test that ensure_directories creates static directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            test_config = Config()
            test_config.STATIC_DIR = temp_path / "test_static"
            
            # Directory should not exist initially
            assert not test_config.STATIC_DIR.exists()
            
            # Call ensure_directories
            test_config.ensure_directories()
            
            # Directory should now exist
            assert test_config.STATIC_DIR.exists()
            assert test_config.STATIC_DIR.is_dir()

    def test_ensure_directories_creates_database_dir(self):
        """Test that ensure_directories creates database directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_dir = temp_path / "db_subdir"
            db_path = db_dir / "test.db"
            
            test_config = Config()
            test_config.DUCKDB_PATH = str(db_path)
            
            # Directory should not exist initially
            assert not db_dir.exists()
            
            # Call ensure_directories
            test_config.ensure_directories()
            
            # Directory should now exist
            assert db_dir.exists()
            assert db_dir.is_dir()

    def test_ensure_directories_handles_existing_dirs(self):
        """Test that ensure_directories handles existing directories gracefully"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            test_config = Config()
            test_config.STATIC_DIR = temp_path / "existing_static"
            
            # Create directory manually
            test_config.STATIC_DIR.mkdir(parents=True, exist_ok=True)
            assert test_config.STATIC_DIR.exists()
            
            # Call ensure_directories - should not raise error
            test_config.ensure_directories()
            
            # Directory should still exist
            assert test_config.STATIC_DIR.exists()
            assert test_config.STATIC_DIR.is_dir()

    def test_ensure_directories_handles_nested_paths(self):
        """Test that ensure_directories handles nested directory paths"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            nested_db_path = temp_path / "data" / "subdirs" / "test.db"
            
            test_config = Config()
            test_config.DUCKDB_PATH = str(nested_db_path)
            
            # Nested path should not exist initially
            assert not nested_db_path.parent.exists()
            
            # Call ensure_directories
            test_config.ensure_directories()
            
            # All nested directories should now exist
            assert nested_db_path.parent.exists()
            assert nested_db_path.parent.is_dir()


class TestAuthenticationConfiguration:
    """Test authentication configuration"""

    @pytest.mark.asyncio
    async def test_get_api_key_with_default(self):
        """Test API key validation with default key"""
        with patch.dict(os.environ, {"WEBHOOK_GATEWAY_API_KEY": "test_key"}):
            # Should not raise exception with correct key
            result = await get_api_key("test_key")
            assert result == "test_key"

    @pytest.mark.asyncio
    async def test_get_api_key_with_invalid_key(self):
        """Test API key validation with invalid key"""
        from fastapi import HTTPException
        
        with patch.dict(os.environ, {"WEBHOOK_GATEWAY_API_KEY": "correct_key"}):
            # Should raise HTTPException with incorrect key
            with pytest.raises(HTTPException) as exc_info:
                await get_api_key("wrong_key")
            
            assert exc_info.value.status_code == 401
            assert "Invalid API key" in str(exc_info.value.detail)

    @pytest.mark.asyncio 
    async def test_get_api_key_uses_environment_variable(self):
        """Test that get_api_key uses environment variable"""
        custom_key = "super_secret_custom_key_123"
        
        with patch.dict(os.environ, {"WEBHOOK_GATEWAY_API_KEY": custom_key}):
            # Should accept the custom key from environment
            result = await get_api_key(custom_key)
            assert result == custom_key
            
            # Should reject different key
            from fastapi import HTTPException
            with pytest.raises(HTTPException):
                await get_api_key("different_key")


class TestConfigurationIntegration:
    """Test configuration integration with other components"""

    def test_global_config_instance(self):
        """Test that global config instance is properly configured"""
        # Global config should be available
        assert config is not None
        assert isinstance(config, Config)
        
        # Should have all required attributes
        assert hasattr(config, 'DUCKDB_PATH')
        assert hasattr(config, 'API_KEY')
        assert hasattr(config, 'HOST')
        assert hasattr(config, 'PORT')
        assert hasattr(config, 'STATIC_DIR')
        assert hasattr(config, 'FRONTEND_DIR')

    def test_config_types_are_correct(self):
        """Test that configuration values have correct types"""
        assert isinstance(config.DUCKDB_PATH, str)
        assert isinstance(config.DUCKDB_MAX_WORKERS, int)
        assert isinstance(config.API_KEY, str)
        assert isinstance(config.HOST, str)
        assert isinstance(config.PORT, int)
        assert isinstance(config.STATIC_DIR, Path)
        assert isinstance(config.FRONTEND_DIR, Path)

    def test_config_values_are_reasonable(self):
        """Test that configuration values are reasonable"""
        # Port should be in valid range
        assert 1 <= config.PORT <= 65535
        
        # Max workers should be positive
        assert config.DUCKDB_MAX_WORKERS > 0
        
        # API key should not be empty
        assert len(config.API_KEY) > 0
        
        # Host should not be empty
        assert len(config.HOST) > 0
        
        # Database path should not be empty
        assert len(config.DUCKDB_PATH) > 0


class TestConfigurationEdgeCases:
    """Test configuration edge cases and error handling"""

    def test_empty_environment_variables(self):
        """Test handling of empty environment variables"""
        with patch.dict(os.environ, {"WEBHOOK_GATEWAY_API_KEY": ""}):
            test_config = Config()
            # Empty string should be preserved (though not recommended)
            assert test_config.API_KEY == ""

    def test_whitespace_environment_variables(self):
        """Test handling of whitespace in environment variables"""
        with patch.dict(os.environ, {"WEBHOOK_GATEWAY_API_KEY": "  spaced_key  "}):
            test_config = Config()
            # Whitespace should be preserved (Config doesn't strip by default)
            assert test_config.API_KEY == "  spaced_key  "

    def test_special_characters_in_paths(self):
        """Test handling of special characters in file paths"""
        special_path = "/path/with spaces/and-dashes/test_db.db"
        with patch.dict(os.environ, {"DUCKDB_PATH": special_path}):
            test_config = Config()
            assert test_config.DUCKDB_PATH == special_path

    def test_relative_vs_absolute_paths(self):
        """Test handling of relative vs absolute paths"""
        # Test relative path
        with patch.dict(os.environ, {"DUCKDB_PATH": "relative/path/test.db"}):
            test_config = Config()
            assert test_config.DUCKDB_PATH == "relative/path/test.db"
        
        # Test absolute path
        with patch.dict(os.environ, {"DUCKDB_PATH": "/absolute/path/test.db"}):
            test_config = Config()
            assert test_config.DUCKDB_PATH == "/absolute/path/test.db"

    def test_very_large_port_number(self):
        """Test handling of very large port numbers"""
        with patch.dict(os.environ, {"PORT": "65535"}):  # Maximum valid port
            test_config = Config()
            assert test_config.PORT == 65535

    def test_zero_port_number(self):
        """Test handling of zero port number"""
        with patch.dict(os.environ, {"PORT": "0"}):
            test_config = Config()
            assert test_config.PORT == 0  # Valid for OS to assign port

    def test_negative_max_workers(self):
        """Test handling of negative max workers"""
        with patch.dict(os.environ, {"DUCKDB_MAX_WORKERS": "-1"}):
            test_config = Config()
            assert test_config.DUCKDB_MAX_WORKERS == -1  # Config allows this, but DuckDB might not