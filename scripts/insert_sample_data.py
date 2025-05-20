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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def insert_sample_data(num_records: int = 100) -> None:
    """Insert sample data into an existing Iceberg table"""
    # Read catalog connection and S3 credentials from environment
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "demo")
    catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    s3_endpoint = os.getenv("S3_ENDPOINT", os.getenv("CATALOG_S3_ENDPOINT", "http://minio:9000"))
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

    logger.info(f"Connecting to Iceberg catalog: {catalog_name} at {catalog_uri}")
    # Pass S3 FileIO config to the catalog so PyIceberg can upload to MinIO
    catalog_props = {
        "type": "rest",
        "uri": catalog_uri,
        "s3.endpoint": s3_endpoint,
        "s3.region": region,
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
        # disable virtual-hosted-style to use path-style requests
        "s3.force-virtual-addressing": "false",
    }
    # Include session token if present
    if session_token:
        catalog_props["s3.session-token"] = session_token

    catalog = load_catalog(name=catalog_name, **catalog_props)

    # Load the table
    table_name = "default.sample_table"
    logger.info(f"Loading table: {table_name}")
    table = catalog.load_table(table_name)

    # Generate sample data
    logger.info(f"Generating {num_records} sample records")
    ids = pa.array(range(1, num_records + 1), type=pa.int64())
    names = pa.array([f"User-{i}" for i in range(1, num_records + 1)], type=pa.string())
    values = pa.array([round(random.uniform(1.0, 1000.0), 2) for _ in range(num_records)], type=pa.float64())
    now = datetime.datetime.now()
    timestamps = pa.array([
        now - datetime.timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        ) for _ in range(num_records)
    ], type=pa.timestamp('us'))
    active = pa.array([random.choice([True, False]) for _ in range(num_records)], type=pa.bool_())

    # Define schema matching the Iceberg table (id is required/non-nullable)
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("value", pa.float64(), nullable=True),
        pa.field("timestamp", pa.timestamp('us'), nullable=True),
        pa.field("active", pa.bool_(), nullable=True),
    ])

    # Create a PyArrow Table with the explicit schema
    pa_table = pa.Table.from_pydict(
        {"id": ids, "name": names, "value": values, "timestamp": timestamps, "active": active},
        schema=schema
    )

    # Append the data to the Iceberg table
    logger.info(f"Writing {num_records} records to table {table_name}")
    table.append(pa_table)
    logger.info(f"Successfully inserted {num_records} records into table {table_name}")

if __name__ == "__main__":
    try:
        # Get number of records from command line argument, default to 100
        num_records = 100
        if len(sys.argv) > 1:
            try:
                num_records = int(sys.argv[1])
            except ValueError:
                logger.warning(
                    f"Invalid number of records: {sys.argv[1]}, using default: 100"
                )
        insert_sample_data(num_records)
    except Exception as e:
        logger.error(f"Failed to insert sample data: {e}")
        sys.exit(1)
