from src.server import mcp, logger
import src.server
import src.tools.table_tools
from src.catalog import IcebergCatalogWrapper
from dotenv import load_dotenv
import os
import sys
import logging

def setup_logging():
    """Set up logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Get log level from environment variable
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Configure file handler
    file_handler = logging.FileHandler("logs/iceberg_mcp.log")
    file_handler.setLevel(logging.DEBUG)  # Always log debug to file
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)  # Use configured log level for console
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Set root logger to debug
    
    # Remove existing handlers if any
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add our handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized with level: {log_level_str}")

def load_config():
    """Load configuration from environment variables"""
    logger.info("Loading configuration from environment variables")
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get catalog type
    catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "rest")
    logger.info(f"Using catalog type: {catalog_type}")
    
    # Get catalog URI
    catalog_uri = os.getenv("ICEBERG_CATALOG_URI")
    if not catalog_uri and catalog_type == "rest":
        logger.warning("ICEBERG_CATALOG_URI not set for REST catalog")
    
    # Create config dictionary
    config = {
        "catalog_name": os.getenv("ICEBERG_CATALOG_NAME", "default"),
        "catalog_config": {
            "type": catalog_type,
            "uri": catalog_uri,
        }
    }
    
    # Add AWS region for Glue catalog
    if catalog_type == "glue":
        aws_region = os.getenv("AWS_REGION")
        if aws_region:
            config["catalog_config"]["region"] = aws_region
            logger.info(f"Using AWS region: {aws_region}")
        else:
            logger.warning("AWS_REGION not set for Glue catalog")
    
    return config

def main():
    """Main entry point for the application"""
    try:
        # Set up logging
        setup_logging()
        logger.info("Starting Iceberg MCP Server")
        
        # Load configuration
        config = load_config()
        
        # Create the catalog wrapper and set it in the server module
        logger.info("Initializing catalog wrapper")
        catalog_wrapper_instance = IcebergCatalogWrapper(config)
        
        # Set catalog_wrapper in both server and table_tools modules
        src.server.catalog_wrapper = catalog_wrapper_instance
        src.tools.table_tools.catalog_wrapper = catalog_wrapper_instance
        
        # Get server port
        port = int(os.getenv("MCP_SERVER_PORT", "8000"))
        logger.info(f"Starting MCP server on port {port}")
        
        # Run the MCP server
        # Note: FastMCP doesn't use the port parameter, it's just for logging
        mcp.run()
    except Exception as e:
        logger.error(f"Error starting Iceberg MCP Server: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
