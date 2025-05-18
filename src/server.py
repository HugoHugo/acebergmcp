from fastmcp import FastMCP
from .catalog import IcebergCatalogWrapper
import logging
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create MCP server
mcp = FastMCP("Iceberg MCP Server")
catalog_wrapper = None  # This will be set by main.py

# Import table_tools after defining mcp and catalog_wrapper
from .tools import table_tools

# Set mcp and catalog_wrapper in table_tools module
table_tools.mcp = mcp
table_tools.catalog_wrapper = catalog_wrapper

@mcp.tool()
async def list_tables(namespace: str = None) -> List[str]:
    """List all tables in the catalog or a specific namespace"""
    if catalog_wrapper is None:
        raise RuntimeError("Catalog wrapper not initialized")
    
    try:
        if namespace:
            logger.info(f"Listing tables in namespace: {namespace}")
            return catalog_wrapper.list_tables(namespace)
        else:
            logger.info("Listing tables in all namespaces")
            tables = []
            for ns in catalog_wrapper.list_namespaces():
                try:
                    tables.extend(catalog_wrapper.list_tables(ns))
                except Exception as e:
                    logger.error(f"Error listing tables in namespace {ns}: {str(e)}")
            return tables
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise RuntimeError(f"Failed to list tables: {str(e)}")

@mcp.resource("table:///{namespace}/{table}")
def get_table_metadata(namespace: str, table: str) -> Dict[str, Any]:
    """Get metadata for a specific table"""
    if catalog_wrapper is None:
        raise RuntimeError("Catalog wrapper not initialized")
    
    try:
        logger.info(f"Getting metadata for table: {namespace}.{table}")
        table_obj = catalog_wrapper.load_table(f"{namespace}.{table}")
        
        # Get current snapshot safely
        current_snapshot_id = None
        try:
            snapshot = table_obj.current_snapshot()
            if snapshot:
                current_snapshot_id = snapshot.snapshot_id
        except Exception as e:
            logger.warning(f"Error getting current snapshot: {str(e)}")
        
        return {
            "schema": str(table_obj.schema()),
            "partition_spec": str(table_obj.spec()),
            "properties": table_obj.properties,
            "current_snapshot": current_snapshot_id
        }
    except Exception as e:
        logger.error(f"Error getting table metadata: {str(e)}")
        raise RuntimeError(f"Failed to get table metadata: {str(e)}")

# Register table tools
mcp.tool()(table_tools.scan_table)
mcp.tool()(table_tools.table_history)
