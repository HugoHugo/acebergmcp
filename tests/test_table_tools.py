import pytest
from unittest.mock import MagicMock, patch, call
from pyiceberg.expressions import EqualTo
from src.tools.table_tools import scan_table, table_history

class TestTableTools:
    """Test the table tools"""
    
    @pytest.mark.asyncio
    async def test_scan_table_no_filters(self, mock_catalog_wrapper, mock_table):
        """Test scanning a table without filters"""
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the scan_table function without filters
            result = await scan_table(namespace="default", table="table1")
        
        # Verify the result
        assert result == [
            {"id": 1, "name": "test1", "value": 10.5},
            {"id": 2, "name": "test2", "value": 20.5},
            {"id": 3, "name": "test3", "value": 30.5}
        ]
        
        # Verify the mock was called correctly
        mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
        mock_table.scan.assert_called_once()
        mock_table.scan().limit.assert_called_once_with(100)
        mock_table.scan().limit().to_arrow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_scan_table_with_filters(self, mock_catalog_wrapper, mock_table):
        """Test scanning a table with filters"""
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the scan_table function with filters
            filters = [
                {"field": "id", "value": 1},
                {"field": "name", "value": "test1"}
            ]
            result = await scan_table(namespace="default", table="table1", filters=filters)
        
        # Verify the result
        assert result == [
            {"id": 1, "name": "test1", "value": 10.5},
            {"id": 2, "name": "test2", "value": 20.5},
            {"id": 3, "name": "test3", "value": 30.5}
        ]
        
        # Verify the mock was called correctly
        mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
        mock_table.scan.assert_called_once()
        
        # Verify filter was called twice (once for each filter)
        assert mock_table.scan().filter.call_count == 2
        
        # Verify limit and to_arrow were called
        mock_table.scan().filter().filter().limit.assert_called_once_with(100)
        mock_table.scan().filter().filter().limit().to_arrow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_scan_table_with_limit(self, mock_catalog_wrapper, mock_table):
        """Test scanning a table with a custom limit"""
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the scan_table function with a custom limit
            result = await scan_table(namespace="default", table="table1", limit=10)
        
        # Verify the result
        assert result == [
            {"id": 1, "name": "test1", "value": 10.5},
            {"id": 2, "name": "test2", "value": 20.5},
            {"id": 3, "name": "test3", "value": 30.5}
        ]
        
        # Verify the mock was called correctly
        mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
        mock_table.scan.assert_called_once()
        mock_table.scan().limit.assert_called_once_with(10)
        mock_table.scan().limit().to_arrow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_scan_table_filter_expression(self, mock_catalog_wrapper, mock_table):
        """Test that the correct filter expression is created"""
        # Patch EqualTo directly in the module being tested
        with patch('src.tools.table_tools.EqualTo') as mock_equal_to:
            # Set up mock
            mock_expr = MagicMock()
            mock_equal_to.return_value = mock_expr
            
            # Reset the filter mock to track calls more accurately
            mock_scan = MagicMock()
            mock_scan.filter.return_value = mock_scan
            mock_scan.limit.return_value = mock_scan
            mock_arrow = MagicMock()
            mock_arrow.to_pylist.return_value = []
            mock_scan.to_arrow.return_value = mock_arrow
            mock_table.scan.return_value = mock_scan
            
            # Patch the logger to avoid actual logging
            with patch('src.tools.table_tools.logger'):
                # Call the scan_table function with a filter
                filters = [{"field": "id", "value": 1}]
                await scan_table(namespace="default", table="table1", filters=filters)
            
            # Verify EqualTo was called correctly
            mock_equal_to.assert_called_once_with("id", 1)
            
            # Verify the filter was applied with the correct expression
            mock_scan.filter.assert_called_once_with(mock_expr)
    
    @pytest.mark.asyncio
    async def test_table_history(self, mock_catalog_wrapper, mock_table):
        """Test getting table history"""
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the table_history function
            result = await table_history(namespace="default", table="table1")
        
        # Verify the result
        assert result == [
            {"snapshot_id": 1234567890, "timestamp_ms": 1620000000000, "operation": "append"},
            {"snapshot_id": 1234567889, "timestamp_ms": 1619999000000, "operation": "append"},
            {"snapshot_id": 1234567888, "timestamp_ms": 1619998000000, "operation": "append"}
        ]
        
        # Verify the mock was called correctly
        mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
        mock_table.history.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_table_history_empty(self, mock_catalog_wrapper, mock_table):
        """Test getting table history when there are no snapshots"""
        # Set up mock to return empty history
        mock_table.history.return_value = []
        
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the table_history function
            result = await table_history(namespace="default", table="table1")
        
        # Verify the result is an empty list
        assert result == []
        
        # Verify the mock was called correctly
        mock_catalog_wrapper.load_table.assert_called_once_with("default.table1")
        mock_table.history.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_scan_table_catalog_wrapper_not_initialized(self):
        """Test scanning a table when catalog_wrapper is not initialized"""
        # Temporarily set catalog_wrapper to None
        import src.tools.table_tools
        original_catalog_wrapper = src.tools.table_tools.catalog_wrapper
        src.tools.table_tools.catalog_wrapper = None
        
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the scan_table function and expect an exception
            with pytest.raises(RuntimeError, match="Catalog wrapper not initialized"):
                await scan_table(namespace="default", table="table1")
        
        # Restore catalog_wrapper
        src.tools.table_tools.catalog_wrapper = original_catalog_wrapper
    
    @pytest.mark.asyncio
    async def test_table_history_catalog_wrapper_not_initialized(self):
        """Test getting table history when catalog_wrapper is not initialized"""
        # Temporarily set catalog_wrapper to None
        import src.tools.table_tools
        original_catalog_wrapper = src.tools.table_tools.catalog_wrapper
        src.tools.table_tools.catalog_wrapper = None
        
        # Patch the logger to avoid actual logging
        with patch('src.tools.table_tools.logger'):
            # Call the table_history function and expect an exception
            with pytest.raises(RuntimeError, match="Catalog wrapper not initialized"):
                await table_history(namespace="default", table="table1")
        
        # Restore catalog_wrapper
        src.tools.table_tools.catalog_wrapper = original_catalog_wrapper
