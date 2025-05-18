#!/usr/bin/env python3
"""
Demo client for the Iceberg MCP Server using SSE Transport.
"""
from fastmcp import Client
from fastmcp.client.transports import SSETransport
import asyncio
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_demo():
    """Run the demo client using SSE transport"""
    try:
        # Get MCP server URL from environment or use default
        port = int(os.getenv("MCP_SERVER_PORT", "8000"))
        sse_url = f"http://localhost:{port}/sse"
        
        logger.info(f"Connecting to MCP server via SSE at {sse_url}")
        
        # Create SSE transport with explicit configuration
        transport = SSETransport(url=sse_url)
        client = Client(transport)
        
        async with client:
            
            # List available tools
            tools = await client.list_tools()
            logger.info(f"Available tools: {', '.join(t.name for t in tools)}")
            
            # List available resources
            resources = await client.list_resources()
            logger.info(f"Available resources: {', '.join(r.uri_pattern for r in resources)}")
            
            # List tables
            logger.info("Listing tables in the default namespace")
            tables = await client.call_tool("list_tables", {"namespace": "default"})
            logger.info(f"Tables in default namespace: {tables}")
            
            if "sample_table" in tables:
                # Get table metadata
                logger.info("Getting metadata for sample_table")
                metadata = await client.access_resource("table:///default/sample_table")
                logger.info(f"Table metadata: {json.dumps(metadata, indent=2)}")
                
                # Scan table
                logger.info("Scanning sample_table (first 5 records)")
                data = await client.call_tool("scan_table", {
                    "namespace": "default",
                    "table": "sample_table",
                    "limit": 5
                })
                logger.info(f"Sample data: {json.dumps(data, indent=2)}")
                
                # Scan table with filter
                logger.info("Scanning sample_table with filter (value > 500)")
                filtered_data = await client.call_tool("scan_table", {
                    "namespace": "default",
                    "table": "sample_table",
                    "filters": [{"field": "value", "op": ">", "value": 500}],
                    "limit": 5
                })
                logger.info(f"Filtered data: {json.dumps(filtered_data, indent=2)}")
                
                # Get table history
                logger.info("Getting history for sample_table")
                history = await client.call_tool("table_history", {
                    "namespace": "default",
                    "table": "sample_table"
                })
                logger.info(f"Table history: {json.dumps(history, indent=2)}")
            
            logger.info("SSE transport demo completed successfully")

    except Exception as e:
        logger.error(f"Error in demo client: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(run_demo())
