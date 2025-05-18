#!/usr/bin/env python3
"""
Script to create a sample Iceberg table for demonstration purposes.
This script connects to the Iceberg REST catalog and creates a sample table.
"""

import os
import sys
import logging
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    DoubleType,
    TimestampType,
    BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sample_table():
    """Create a sample Iceberg table"""
    try:
        # Load the catalog
        catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "demo")
        catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        
        logger.info(f"Connecting to Iceberg catalog: {catalog_name} at {catalog_uri}")
        catalog = load_catalog(
            name=catalog_name,
            type="rest",
            uri=catalog_uri
        )
        
        # Define the schema
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType()),
            NestedField(3, "value", DoubleType()),
            NestedField(4, "timestamp", TimestampType()),
            NestedField(5, "active", BooleanType())
        )
        
        # Define the partition spec
        partition_spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, name="id", transform="identity")
        )
        
        # Create the namespace if it doesn't exist
        namespace = "default"
        if (namespace,) not in catalog.list_namespaces():
            logger.info(f"Creating namespace: {namespace}")
            catalog.create_namespace(namespace)
        
        # Create the table
        table_name = f"{namespace}.sample_table"
        logger.info(f"Creating table: {table_name}")
        
        # Check if table already exists
        try:
            existing_table = catalog.load_table(table_name)
            logger.info(f"Table {table_name} already exists, skipping creation")
            return existing_table
        except Exception:
            # Table doesn't exist, create it
            table = catalog.create_table(
                identifier=table_name,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.info(f"Table {table_name} created successfully")
            return table
    
    except Exception as e:
        logger.error(f"Error creating sample table: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        create_sample_table()
    except Exception as e:
        logger.error(f"Failed to create sample table: {str(e)}")
        sys.exit(1)
