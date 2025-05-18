from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table
from typing import List, Dict, Any, Optional
import logging

# Get logger
logger = logging.getLogger(__name__)

# These will be set when the module is imported by server.py
mcp = None
catalog_wrapper = None

async def scan_table(
    namespace: str, 
    table: str, 
    filters: Optional[List[Dict[str, Any]]] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Scan a table with optional filters"""
    if catalog_wrapper is None:
        raise RuntimeError("Catalog wrapper not initialized")
    
    try:
        logger.info(f"Scanning table: {namespace}.{table}")
        table_obj = catalog_wrapper.load_table(f"{namespace}.{table}")
        scan = table_obj.scan()
        
        if filters:
            logger.info(f"Applying filters: {filters}")
            # Convert filter dicts to PyIceberg expressions
            for filter_dict in filters:
                try:
                    field = filter_dict["field"]
                    value = filter_dict["value"]
                    scan = scan.filter(EqualTo(field, value))
                except KeyError as e:
                    logger.error(f"Invalid filter format: {filter_dict}. Error: {str(e)}")
                    raise ValueError(f"Filter must contain 'field' and 'value' keys: {filter_dict}")
                except Exception as e:
                    logger.error(f"Error applying filter {filter_dict}: {str(e)}")
                    raise RuntimeError(f"Failed to apply filter: {str(e)}")
        
        logger.info(f"Executing scan with limit: {limit}")
        return scan.limit(limit).to_arrow().to_pylist()
    except Exception as e:
        logger.error(f"Error scanning table: {str(e)}")
        raise RuntimeError(f"Failed to scan table: {str(e)}")

async def table_history(namespace: str, table: str) -> List[Dict[str, Any]]:
    """Get snapshot history of a table"""
    if catalog_wrapper is None:
        raise RuntimeError("Catalog wrapper not initialized")
    
    try:
        logger.info(f"Getting history for table: {namespace}.{table}")
        table_obj = catalog_wrapper.load_table(f"{namespace}.{table}")
        
        history = []
        for snapshot in table_obj.history():
            try:
                history.append({
                    "snapshot_id": snapshot.snapshot_id,
                    "timestamp_ms": snapshot.timestamp_ms,
                    "operation": snapshot.summary.get("operation", "unknown")
                })
            except Exception as e:
                logger.warning(f"Error processing snapshot: {str(e)}")
        
        return history
    except Exception as e:
        logger.error(f"Error getting table history: {str(e)}")
        raise RuntimeError(f"Failed to get table history: {str(e)}")
