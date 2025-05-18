#!/usr/bin/env python3
"""
Script to insert sample data into the Iceberg table.
This script connects to the Iceberg REST catalog and inserts sample data into the table.
"""

import os
import sys
import logging
import random
import datetime
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pyarrow.compute as pc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def insert_sample_data(num_records=100):
    """Insert sample data into the Iceberg table"""
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
        
        # Load the table
        table_name = "default.sample_table"
        logger.info(f"Loading table: {table_name}")
        table = catalog.load_table(table_name)
        
        # Generate sample data
        logger.info(f"Generating {num_records} sample records")
        
        # Create PyArrow arrays for each column
        ids = pa.array(range(1, num_records + 1), type=pa.int64())
        
        names = pa.array([
            f"User-{i}" for i in range(1, num_records + 1)
        ], type=pa.string())
        
        values = pa.array([
            round(random.uniform(1.0, 1000.0), 2) for _ in range(num_records)
        ], type=pa.float64())
        
        now = datetime.datetime.now()
        timestamps = pa.array([
            now - datetime.timedelta(days=random.randint(0, 30), 
                                    hours=random.randint(0, 23), 
                                    minutes=random.randint(0, 59), 
                                    seconds=random.randint(0, 59))
            for _ in range(num_records)
        ], type=pa.timestamp('us'))
        
        active = pa.array([
            random.choice([True, False]) for _ in range(num_records)
        ], type=pa.bool_())
        
        # Create a PyArrow table
        pa_table = pa.Table.from_arrays(
            [ids, names, values, timestamps, active],
            names=["id", "name", "value", "timestamp", "active"]
        )
        
        # Write the data to the Iceberg table
        logger.info(f"Writing {num_records} records to table {table_name}")
        with table.new_append() as append:
            append.append_table(pa_table)
        
        logger.info(f"Successfully inserted {num_records} records into table {table_name}")
    
    except Exception as e:
        logger.error(f"Error inserting sample data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Get number of records from command line argument, default to 100
        num_records = 100
        if len(sys.argv) > 1:
            try:
                num_records = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Invalid number of records: {sys.argv[1]}, using default: 100")
        
        insert_sample_data(num_records)
    except Exception as e:
        logger.error(f"Failed to insert sample data: {str(e)}")
        sys.exit(1)
