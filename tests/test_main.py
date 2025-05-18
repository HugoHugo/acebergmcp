import pytest
from unittest.mock import MagicMock, patch, call
import os
import sys
import logging

class TestMain:
    """Test the main module"""
    
    @patch('src.main.IcebergCatalogWrapper')
    @patch('src.main.mcp')
    @patch('src.main.load_dotenv')
    @patch('src.main.setup_logging')
    @patch.dict(os.environ, {
        "ICEBERG_CATALOG_NAME": "test_catalog",
        "ICEBERG_CATALOG_TYPE": "rest",
        "ICEBERG_CATALOG_URI": "http://localhost:8181",
        "MCP_SERVER_PORT": "8000"
    })
    def test_main_function(self, mock_setup_logging, mock_load_dotenv, mock_mcp, mock_catalog_wrapper_class):
        """Test that the main function initializes correctly"""
        # Set up mock
        mock_catalog_wrapper = MagicMock()
        mock_catalog_wrapper_class.return_value = mock_catalog_wrapper
        
        # Patch sys.exit to prevent test from exiting
        with patch('sys.exit'):
            # Import main module
            from src.main import main
            
            # Call the main function
            main()
        
        # Verify setup_logging was called
        mock_setup_logging.assert_called_once()
        
        # Verify dotenv was loaded
        mock_load_dotenv.assert_called_once()
        
        # Verify catalog wrapper was created with correct config
        mock_catalog_wrapper_class.assert_called_once()
        config_arg = mock_catalog_wrapper_class.call_args[0][0]
        assert config_arg["catalog_name"] == "test_catalog"
        assert config_arg["catalog_config"]["type"] == "rest"
        assert config_arg["catalog_config"]["uri"] == "http://localhost:8181"
        
        # Verify MCP server was run
        mock_mcp.run.assert_called_once()
    
    @patch('src.main.IcebergCatalogWrapper')
    @patch('src.main.mcp')
    @patch('src.main.load_dotenv')
    @patch('src.main.setup_logging')
    @patch.dict(os.environ, {
        "ICEBERG_CATALOG_NAME": "glue_catalog",
        "ICEBERG_CATALOG_TYPE": "glue",
        "ICEBERG_CATALOG_URI": "",
        "AWS_REGION": "us-west-2",
        "MCP_SERVER_PORT": "9000"
    })
    def test_main_function_glue(self, mock_setup_logging, mock_load_dotenv, mock_mcp, mock_catalog_wrapper_class):
        """Test that the main function initializes correctly with Glue catalog"""
        # Set up mock
        mock_catalog_wrapper = MagicMock()
        mock_catalog_wrapper_class.return_value = mock_catalog_wrapper
        
        # Patch sys.exit to prevent test from exiting
        with patch('sys.exit'):
            # Import main module
            from src.main import main
            
            # Call the main function
            main()
        
        # Verify catalog wrapper was created with correct config
        mock_catalog_wrapper_class.assert_called_once()
        config_arg = mock_catalog_wrapper_class.call_args[0][0]
        assert config_arg["catalog_name"] == "glue_catalog"
        assert config_arg["catalog_config"]["type"] == "glue"
        assert config_arg["catalog_config"]["uri"] == ""
        
        # Verify MCP server was run
        mock_mcp.run.assert_called_once()
    
    @patch('src.main.IcebergCatalogWrapper')
    @patch('src.main.mcp')
    @patch('src.main.load_dotenv')
    @patch('src.main.setup_logging')
    @patch.dict(os.environ, {
        "ICEBERG_CATALOG_TYPE": "rest",
        "MCP_SERVER_PORT": "8000"
    })
    def test_main_function_defaults(self, mock_setup_logging, mock_load_dotenv, mock_mcp, mock_catalog_wrapper_class):
        """Test that the main function initializes with defaults when env vars are missing"""
        # Set up mock
        mock_catalog_wrapper = MagicMock()
        mock_catalog_wrapper_class.return_value = mock_catalog_wrapper
        
        # Patch sys.exit to prevent test from exiting
        with patch('sys.exit'):
            # Import main module
            from src.main import main
            
            # Call the main function
            main()
        
        # Verify catalog wrapper was created with correct config
        mock_catalog_wrapper_class.assert_called_once()
        config_arg = mock_catalog_wrapper_class.call_args[0][0]
        assert config_arg["catalog_name"] == "default"
        
        # Verify MCP server was run
        mock_mcp.run.assert_called_once()
    
    @patch('logging.FileHandler')
    @patch('logging.StreamHandler')
    @patch('os.makedirs')
    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"})
    def test_setup_logging(self, mock_makedirs, mock_stream_handler, mock_file_handler):
        """Test that setup_logging configures logging correctly"""
        # Set up mocks
        mock_file_handler_instance = MagicMock()
        mock_file_handler_instance.level = logging.DEBUG
        mock_file_handler.return_value = mock_file_handler_instance
        
        mock_stream_handler_instance = MagicMock()
        mock_stream_handler_instance.level = logging.DEBUG
        mock_stream_handler.return_value = mock_stream_handler_instance
        
        # Import and call setup_logging
        from src.main import setup_logging
        
        # Patch the logger to avoid actual logging
        with patch('src.main.logger'):
            setup_logging()
        
        # Verify logs directory was created
        mock_makedirs.assert_called_once_with("logs", exist_ok=True)
        
        # Verify file handler was configured correctly
        mock_file_handler.assert_called_once_with("logs/iceberg_mcp.log")
        mock_file_handler_instance.setLevel.assert_called_once_with(logging.DEBUG)
        
        # Verify stream handler was configured correctly
        mock_stream_handler.assert_called_once()
        mock_stream_handler_instance.setLevel.assert_called_once_with(logging.DEBUG)
