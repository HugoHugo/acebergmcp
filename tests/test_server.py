import pytest
from unittest.mock import MagicMock, patch, call
from pyiceberg.expressions import EqualTo
from src.server import list_tables, get_table_metadata

class TestMCPServer:
    """Test the MCP server tools and resources"""
    
    @pytest.mark.asyncio
    async def test_list_tables_all(self, mock_catalog_wrapper):
        """Test listing all tables across all namespaces"""
        # Set up mock to return expected values
        mock_catalog_wrapper.list_namespaces.return_value = [("default",), ("test",)]
        mock_catalog_wrapper.list_tables.side_effect = lambda ns: ["table1", "table2"]
        
        # Patch the server module to use our mock
        with patch('src.server.catalog_wrapper', mock_catalog_wrapper):
            # Patch the logger to avoid actual logging
            with patch('src.server.logger'):
                # Call the list_tables function without a namespace
                result = await list_tables()
            
            # Verify the result
            assert result == ["table1", "table2", "table1", "table2"]
            
            # Verify the mock was called correctly
            mock_catalog_wrapper.list_namespaces.assert_called_once()
            assert mock_catalog_wrapper.list_tables.call_count == 2
    
    @pytest.mark.asyncio
    async def test_list_tables_namespace(self, mock_catalog_wrapper):
        """Test listing tables in a specific namespace"""
        # Set up mock to return expected values
        mock_catalog_wrapper.list_tables.return_value = ["table1", "table2"]
        
        # Patch the server module to use our mock
        with patch('src.server.catalog_wrapper', mock_catalog_wrapper):
            # Patch the logger to avoid actual logging
            with patch('src.server.logger'):
                # Call the list_tables function with a namespace
                result = await list_tables(namespace="default")
            
            # Verify the result
            assert result == ["table1", "table2"]
            
            # Verify the mock was called correctly
            mock_catalog_wrapper.list_tables.assert_called_once_with("default")
    
    def test_get_table_metadata(self, mock_catalog_wrapper, mock_table):
        """Test getting table metadata"""
        # Patch the server module to use our mock
        with patch('src.server.catalog_wrapper', mock_catalog_wrapper):
            # Patch the logger to avoid actual logging
            with patch('src.server.logger'):
                # Call the get_table_metadata function
                result = get_table_metadata(namespace="default", table="table1")
            
            # Verify the result
            assert result == {
                "schema": "table_id(1): struct<id: int, name: string, value: double>",
                "partition_spec": "[\n  id: 1000 -> 1000: id as identity\n]",
                "properties": {
                    "format-version": "2",
                    "location": "s3://bucket/warehouse/default/table1",
                    "table-uuid": "12345678-1234-1234-1234-123456789012"
                },
                "current_snapshot": 1234567890
            }
            
            # Verify the mock was called correctly
            mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
    
    @pytest.mark.asyncio
    async def test_list_tables_catalog_wrapper_not_initialized(self):
        """Test listing tables when catalog_wrapper is not initialized"""
        # Temporarily set catalog_wrapper to None
        import src.server
        original_catalog_wrapper = src.server.catalog_wrapper
        src.server.catalog_wrapper = None
        
        # Patch the logger to avoid actual logging
        with patch('src.server.logger'):
            # Call the list_tables function and expect an exception
            with pytest.raises(RuntimeError, match="Catalog wrapper not initialized"):
                await list_tables()
        
        # Restore catalog_wrapper
        src.server.catalog_wrapper = original_catalog_wrapper
    
    def test_get_table_metadata_catalog_wrapper_not_initialized(self):
        """Test getting table metadata when catalog_wrapper is not initialized"""
        # Temporarily set catalog_wrapper to None
        import src.server
        original_catalog_wrapper = src.server.catalog_wrapper
        src.server.catalog_wrapper = None
        
        # Patch the logger to avoid actual logging
        with patch('src.server.logger'):
            # Call the get_table_metadata function and expect an exception
            with pytest.raises(RuntimeError, match="Catalog wrapper not initialized"):
                get_table_metadata(namespace="default", table="table1")
        
        # Restore catalog_wrapper
        src.server.catalog_wrapper = original_catalog_wrapper
    
    @pytest.mark.asyncio
    async def test_list_tables_namespace_error(self):
        """Test listing tables in a namespace that raises an error"""
        # Create a mock catalog wrapper
        mock_catalog_wrapper = MagicMock()
        
        # Set up mock to return expected values for namespaces
        mock_catalog_wrapper.list_namespaces.return_value = [("default",), ("test",)]
        
        # Set up mock to raise an exception for a specific namespace
        def list_tables_side_effect(ns):
            if ns[0] == "default":
                return ["table1", "table2"]
            else:
                raise RuntimeError(f"Error listing tables in {ns}")
        
        mock_catalog_wrapper.list_tables.side_effect = list_tables_side_effect
        
        # Patch the server module to use our mock
        with patch('src.server.catalog_wrapper', mock_catalog_wrapper):
            # Patch the logger to avoid actual logging
            with patch('src.server.logger'):
                # Call the list_tables function without a namespace
                # This should still return tables from the default namespace
                result = await list_tables()
            
            # Verify the result contains only tables from the default namespace
            assert result == ["table1", "table2"]
            
            # Verify the mock was called correctly
            mock_catalog_wrapper.list_namespaces.assert_called_once()
            assert mock_catalog_wrapper.list_tables.call_count >= 1
    
    def test_get_table_metadata_no_current_snapshot(self, mock_catalog_wrapper, mock_table):
        """Test getting table metadata when there is no current snapshot"""
        # Set up mock to return None for current_snapshot
        mock_table.current_snapshot.return_value = None
        
        # Patch the server module to use our mock
        with patch('src.server.catalog_wrapper', mock_catalog_wrapper):
            # Patch the logger to avoid actual logging
            with patch('src.server.logger'):
                # Call the get_table_metadata function
                result = get_table_metadata(namespace="default", table="table1")
            
            # Verify the result has current_snapshot set to None
            assert result["current_snapshot"] is None
            
            # Verify the mock was called correctly
            mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
