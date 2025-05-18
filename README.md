# Iceberg MCP Server

A Model Context Protocol (MCP) server for Apache Iceberg that provides tools and resources for interacting with Iceberg tables.

## Features

- List tables in an Iceberg catalog
- Scan tables with filters
- Get table history
- Get table metadata

## Prerequisites

- Docker (with Docker Compose support)
- Python 3.10+ (for local development)

## Quick Start with Docker Compose

The easiest way to get started is to use Docker Compose, which will set up all the necessary components:

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/iceberg-mcp-server.git
   cd iceberg-mcp-server
   ```

2. Start the demo environment:
   ```bash
   ./run_demo.sh
   ```

   This will:
   - Start a MinIO server for S3 storage
   - Start an Iceberg REST catalog
   - Create a sample table
   - Insert sample data
   - Start the Iceberg MCP server
   - Run a demo client

3. The MCP server will be available at `http://localhost:8000`

4. To stop all services when you're done:
   ```bash
   docker compose -f docker-compose.demo.yml down
   ```

## Using the MCP Server

The MCP server provides the following tools and resources:

### Tools

- `list_tables`: List all tables in a catalog or specific namespace
  ```python
  tables = await mcp.use_tool("list_tables", namespace="default")
  ```

- `scan_table`: Scan a table with optional filters
  ```python
  data = await mcp.use_tool("scan_table", namespace="default", table="sample_table", limit=10)
  ```

- `table_history`: Get snapshot history of a table
  ```python
  history = await mcp.use_tool("table_history", namespace="default", table="sample_table")
  ```

### Resources

- `table:///{namespace}/{table}`: Get metadata for a specific table
  ```python
  metadata = await mcp.access_resource(f"table:///default/sample_table")
  ```

## Development Setup

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run tests:
   ```bash
   python -m pytest
   ```

4. Start the server locally:
   ```bash
   python -m src.main
   ```

## Docker Compose Configurations

- `docker-compose.yml`: Basic setup with MinIO, Iceberg REST catalog, and the MCP server
- `docker-compose.demo.yml`: Demo setup that also creates a sample table and inserts data

## Environment Variables

The server can be configured using the following environment variables:

- `ICEBERG_CATALOG_NAME`: Name of the Iceberg catalog (default: "default")
- `ICEBERG_CATALOG_TYPE`: Type of the Iceberg catalog (default: "rest")
- `ICEBERG_CATALOG_URI`: URI of the Iceberg catalog (default: "http://localhost:8181")
- `MCP_SERVER_PORT`: Port for the MCP server (default: 8000)
- `LOG_LEVEL`: Logging level (default: "INFO")

## Troubleshooting

If you encounter any issues:

1. Check the logs of specific services:
   ```bash
   docker compose -f docker-compose.demo.yml logs iceberg-rest
   docker compose -f docker-compose.demo.yml logs iceberg-mcp-server
   ```

2. Ensure all services are running:
   ```bash
   docker compose -f docker-compose.demo.yml ps
   ```

3. If a service fails, you can restart it:
   ```bash
   docker compose -f docker-compose.demo.yml restart <service-name>
   ```

## License

[Apache License 2.0](LICENSE)
