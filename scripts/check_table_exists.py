#!/usr/bin/env python3
import os
import sys
import logging
from pyiceberg.catalog import load_catalog

# Optional: configure logging if you want debug output
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def main():
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "demo")
    catalog_uri  = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    table_name   = "default.sample_table"

    logger.info(f"Checking for table {table_name} in catalog {catalog_name} @ {catalog_uri}")
    catalog = load_catalog(
        name=catalog_name,
        type="rest",
        uri=catalog_uri,
    )

    try:
        catalog.load_table(table_name)
        logger.info("Table exists!")
        sys.exit(0)
    except Exception as e:
        logger.debug(f"Table not found: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
