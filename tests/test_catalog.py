import pytest
from unittest.mock import MagicMock, patch
from pyiceberg.catalog import Catalog
from src.catalog import IcebergCatalogWrapper

class TestIcebergCatalogWrapper:
    """Test the IcebergCatalogWrapper class"""
    
    @patch('src.catalog.load_catalog')
    def test_init_default_config(self, mock_load_catalog):
        """Test initializing with default config"""
        # Set up mock
        mock_catalog = MagicMock(spec=Catalog)
        mock_load_catalog.return_value = mock_catalog
        
        # Create wrapper with minimal config
        config = {
            "catalog_name": "test_catalog"
        }
        wrapper = IcebergCatalogWrapper(config)
        
        # Verify the catalog was loaded correctly
        mock_load_catalog.assert_called_once_with(
            name="test_catalog",
            **{}
        )
        assert wrapper.catalog == mock_catalog
    
    @patch('src.catalog.load_catalog')
    def test_init_full_config(self, mock_load_catalog):
        """Test initializing with full config"""
        # Set up mock
        mock_catalog = MagicMock(spec=Catalog)
        mock_load_catalog.return_value = mock_catalog
        
        # Create wrapper with full config
        config = {
            "catalog_name": "test_catalog",
            "catalog_config": {
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "s3://bucket/warehouse"
            }
        }
        wrapper = IcebergCatalogWrapper(config)
        
        # Verify the catalog was loaded correctly
        mock_load_catalog.assert_called_once_with(
            name="test_catalog",
            type="rest",
            uri="http://localhost:8181",
            warehouse="s3://bucket/warehouse"
        )
        assert wrapper.catalog == mock_catalog
    
    @patch('src.catalog.load_catalog')
    def test_list_namespaces(self, mock_load_catalog):
        """Test listing namespaces"""
        # Set up mock
        mock_catalog = MagicMock(spec=Catalog)
        mock_catalog.list_namespaces.return_value = [("default",), ("test",)]
        mock_load_catalog.return_value = mock_catalog
        
        # Create wrapper
        wrapper = IcebergCatalogWrapper({"catalog_name": "test_catalog"})
        
        # Call the method
        result = wrapper.list_namespaces()
        
        # Verify the result
        assert result == [("default",), ("test",)]
        mock_catalog.list_namespaces.assert_called_once()
    
    @patch('src.catalog.load_catalog')
    def test_list_tables(self, mock_load_catalog):
        """Test listing tables"""
        # Set up mock
        mock_catalog = MagicMock(spec=Catalog)
        mock_catalog.list_tables.return_value = ["default.table1", "default.table2"]
        mock_load_catalog.return_value = mock_catalog
        
        # Create wrapper
        wrapper = IcebergCatalogWrapper({"catalog_name": "test_catalog"})
        
        # Call the method
        result = wrapper.list_tables("default")
        
        # Verify the result
        assert result == ["default.table1", "default.table2"]
        mock_catalog.list_tables.assert_called_once_with("default")
    
    @patch('src.catalog.load_catalog')
    def test_load_table(self, mock_load_catalog):
        """Test loading a table"""
        # Set up mock
        mock_catalog = MagicMock(spec=Catalog)
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog
        
        # Create wrapper
        wrapper = IcebergCatalogWrapper({"catalog_name": "test_catalog"})
        
        # Call the method
        result = wrapper.load_table("default.table1")
        
        # Verify the result
        assert result == mock_table
        mock_catalog.load_table.assert_called_once_with("default.table1")
