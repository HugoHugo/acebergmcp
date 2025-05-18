from pyiceberg.catalog import load_catalog
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

class IcebergCatalogWrapper:
    """Wrapper for PyIceberg catalog to provide error handling and logging"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the catalog wrapper with the given configuration"""
        try:
            catalog_name = config.get("catalog_name", "default")
            catalog_config = config.get("catalog_config", {})
            
            logger.info(f"Initializing Iceberg catalog: {catalog_name}")
            logger.debug(f"Catalog config: {catalog_config}")
            
            self.catalog = load_catalog(
                name=catalog_name,
                **catalog_config
            )
            logger.info(f"Successfully initialized Iceberg catalog: {catalog_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {str(e)}")
            raise RuntimeError(f"Failed to initialize Iceberg catalog: {str(e)}")
    
    def list_namespaces(self) -> List[Tuple[str, ...]]:
        """List all namespaces in the catalog"""
        try:
            logger.info("Listing namespaces")
            namespaces = self.catalog.list_namespaces()
            logger.debug(f"Found namespaces: {namespaces}")
            return namespaces
        except Exception as e:
            logger.error(f"Failed to list namespaces: {str(e)}")
            raise RuntimeError(f"Failed to list namespaces: {str(e)}")
    
    def list_tables(self, namespace: str) -> List[str]:
        """List all tables in the given namespace"""
        try:
            logger.info(f"Listing tables in namespace: {namespace}")
            tables = self.catalog.list_tables(namespace)
            logger.debug(f"Found tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables in namespace {namespace}: {str(e)}")
            raise RuntimeError(f"Failed to list tables in namespace {namespace}: {str(e)}")
    
    def load_table(self, identifier: str) -> Any:
        """Load a table with the given identifier"""
        try:
            logger.info(f"Loading table: {identifier}")
            table = self.catalog.load_table(identifier)
            logger.debug(f"Successfully loaded table: {identifier}")
            return table
        except Exception as e:
            logger.error(f"Failed to load table {identifier}: {str(e)}")
            raise RuntimeError(f"Failed to load table {identifier}: {str(e)}")
