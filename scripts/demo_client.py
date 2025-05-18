#!/usr/bin/env python3
"""
Demo client for the Iceberg MCP Server.
This script demonstrates how to use the MCP server to interact with Iceberg tables.
"""

import asyncio
import json
import logging
import os
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleMCPClient:
    """A simple MCP client implementation that doesn't rely on fastmcp"""
    
    def __init__(self, base_url):
        self.base_url = base_url
        
    async def get_server_info(self):
        """Get server information"""
        response = requests.get(f"{self.base_url}/")
        response.raise_for_status()
        return response.json()
    
    async def list_tools(self):
        """List available tools"""
        response = requests.get(f"{self.base_url}/tools")
        response.raise_for_status()
        return response.json()
    
    async def list_resources(self):
        """List available resources"""
        response = requests.get(f"{self.base_url}/resources")
        response.raise_for_status()
        return response.json()
    
    async def use_tool(self, tool_name, arguments=None):
        """Use a tool"""
        if arguments is None:
            arguments = {}
        response = requests.post(
            f"{self.base_url}/tools/{tool_name}",
            json=arguments
        )
        response.raise_for_status()
        return response.json()
    
    async def access_resource(self, uri):
        """Access a resource"""
        # URL encode the URI
        encoded_uri = requests.utils.quote(uri, safe='')
        response = requests.get(f"{self.base_url}/resources/{encoded_uri}")
        response.raise_for_status()
        return response.json()

async def run_demo():
    """Run the demo client"""
    try:
        # Get MCP server URL from environment or use default
        mcp_server_url = os.getenv("MCP_SERVER_URL", "http://iceberg-mcp-server:8000")
        logger.info(f"Connecting to MCP server at {mcp_server_url}")
        client = SimpleMCPClient(mcp_server_url)
        
        # Get server info
        server_info = await client.get_server_info()
        logger.info(f"Connected to MCP server: {server_info['name']}")
        
        # List available tools
        tools = await client.list_tools()
        logger.info(f"Available tools: {', '.join(tool['name'] for tool in tools)}")
        
        # List available resources
        resources = await client.list_resources()
        logger.info(f"Available resources: {', '.join(resource['uri_pattern'] for resource in resources)}")
        
        # List tables
        logger.info("Listing tables in the default namespace")
        tables = await client.use_tool("list_tables", {"namespace": "default"})
        logger.info(f"Tables in default namespace: {tables}")
        
        if "sample_table" in tables:
            # Get table metadata
            logger.info("Getting metadata for sample_table")
            metadata = await client.access_resource("table:///default/sample_table")
            logger.info(f"Table metadata: {json.dumps(metadata, indent=2)}")
            
            # Scan table
            logger.info("Scanning sample_table (first 5 records)")
            data = await client.use_tool("scan_table", {
                "namespace": "default",
                "table": "sample_table",
                "limit": 5
            })
            logger.info(f"Sample data: {json.dumps(data, indent=2)}")
            
            # Scan table with filter
            logger.info("Scanning sample_table with filter (value > 500)")
            filtered_data = await client.use_tool("scan_table", {
                "namespace": "default",
                "table": "sample_table",
                "filters": [{"field": "value", "op": ">", "value": 500}],
                "limit": 5
            })
            logger.info(f"Filtered data: {json.dumps(filtered_data, indent=2)}")
            
            # Get table history
            logger.info("Getting history for sample_table")
            history = await client.use_tool("table_history", {
                "namespace": "default",
                "table": "sample_table"
            })
            logger.info(f"Table history: {json.dumps(history, indent=2)}")
        
        logger.info("Demo completed successfully")
    
    except Exception as e:
        logger.error(f"Error in demo client: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(run_demo())
