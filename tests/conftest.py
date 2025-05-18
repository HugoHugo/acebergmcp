import pytest
from unittest.mock import MagicMock, patch
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.catalog import Catalog
from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestFile
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from src.catalog import IcebergCatalogWrapper
import src.server
import src.tools.table_tools

@pytest.fixture
def mock_catalog():
    """Create a mock Iceberg catalog"""
    mock_catalog = MagicMock(spec=Catalog)
    
    # Set up namespaces
    mock_catalog.list_namespaces.return_value = [("default",), ("test",)]
    
    # Set up tables
    mock_catalog.list_tables.side_effect = lambda namespace: [
        f"{namespace[0]}.table1",
        f"{namespace[0]}.table2"
    ] if namespace in [("default",), ("test",)] else []
    
    return mock_catalog

@pytest.fixture
def mock_table():
    """Create a mock Iceberg table"""
    mock_table = MagicMock(spec=Table)
    
    # Set up schema
    mock_schema = MagicMock(spec=Schema)
    mock_schema.__str__.return_value = "table_id(1): struct<id: int, name: string, value: double>"
    mock_table.schema.return_value = mock_schema
    
    # Set up partition spec
    mock_spec = MagicMock(spec=PartitionSpec)
    mock_spec.__str__.return_value = "[\n  id: 1000 -> 1000: id as identity\n]"
    mock_table.spec.return_value = mock_spec
    
    # Set up properties
    mock_table.properties = {
        "format-version": "2",
        "location": "s3://bucket/warehouse/default/table1",
        "table-uuid": "12345678-1234-1234-1234-123456789012"
    }
    
    # Set up current snapshot
    mock_snapshot = MagicMock(spec=Snapshot)
    mock_snapshot.snapshot_id = 1234567890
    mock_snapshot.timestamp_ms = 1620000000000
    mock_snapshot.summary = {"operation": "append"}
    mock_table.current_snapshot.return_value = mock_snapshot
    
    # Set up history
    mock_table.history.return_value = [
        mock_snapshot,
        MagicMock(spec=Snapshot, snapshot_id=1234567889, timestamp_ms=1619999000000, 
                  summary={"operation": "append"}),
        MagicMock(spec=Snapshot, snapshot_id=1234567888, timestamp_ms=1619998000000, 
                  summary={"operation": "append"})
    ]
    
    # Set up scan
    mock_scan = MagicMock()
    mock_scan.filter.return_value = mock_scan
    mock_scan.limit.return_value = mock_scan
    
    # Mock Arrow conversion
    mock_arrow = MagicMock()
    mock_arrow.to_pylist.return_value = [
        {"id": 1, "name": "test1", "value": 10.5},
        {"id": 2, "name": "test2", "value": 20.5},
        {"id": 3, "name": "test3", "value": 30.5}
    ]
    mock_scan.to_arrow.return_value = mock_arrow
    
    mock_table.scan.return_value = mock_scan
    
    return mock_table

@pytest.fixture
def mock_catalog_wrapper(mock_catalog, mock_table):
    """Create a mock IcebergCatalogWrapper"""
    mock_wrapper = MagicMock(spec=IcebergCatalogWrapper)
    mock_wrapper.catalog = mock_catalog
    
    # Set up list_namespaces
    mock_wrapper.list_namespaces.return_value = [("default",), ("test",)]
    
    # Set up list_tables
    mock_wrapper.list_tables.side_effect = lambda namespace: [
        f"table1",
        f"table2"
    ] if namespace in ["default", "test"] else []
    
    # Set up load_table
    mock_wrapper.load_table.return_value = mock_table
    
    return mock_wrapper

@pytest.fixture(autouse=True)
def setup_mcp_server(mock_catalog_wrapper):
    """Set up the MCP server with mock catalog wrapper"""
    # Save original catalog_wrapper
    original_server_catalog_wrapper = src.server.catalog_wrapper
    original_tools_catalog_wrapper = src.tools.table_tools.catalog_wrapper
    
    # Set mock catalog_wrapper
    src.server.catalog_wrapper = mock_catalog_wrapper
    src.tools.table_tools.catalog_wrapper = mock_catalog_wrapper
    
    yield
    
    # Restore original catalog_wrapper
    src.server.catalog_wrapper = original_server_catalog_wrapper
    src.tools.table_tools.catalog_wrapper = original_tools_catalog_wrapper
