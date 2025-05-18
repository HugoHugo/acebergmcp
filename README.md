# Apache Iceberg MCP Server

An MCP (Model Context Protocol) server for Apache Iceberg that provides tools and resources for interacting with Iceberg tables.

## Overview

This MCP server provides a set of tools and resources for working with Apache Iceberg tables. It uses the PyIceberg library to interact with Iceberg catalogs and tables, and exposes functionality through the MCP protocol.

## Features

- List tables in a catalog or specific namespace
- Scan tables with optional filters
- Get table history (snapshots)
- Get table metadata (schema, partition spec, properties)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/iceberg-mcp-server.git
cd iceberg-mcp-server
```

2. Create a Python virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

The server is configured using environment variables, which can be set in a `.env` file:

```
# Catalog Configuration
ICEBERG_CATALOG_NAME=default
ICEBERG_CATALOG_TYPE=rest  # Options: rest, glue, hive, etc.
ICEBERG_CATALOG_URI=http://localhost:8181
ICEBERG_CATALOG_NAMESPACE=default

# AWS Configuration (for Glue catalog)
AWS_REGION=us-east-1
# AWS_ACCESS_KEY_ID=your_access_key  # Uncomment if not using instance profile
# AWS_SECRET_ACCESS_KEY=your_secret_key  # Uncomment if not using instance profile

# Server Configuration
MCP_SERVER_PORT=8000  # Port for the MCP server
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

## Usage

### Starting the Server

```bash
python -m src.main
```

### Using the MCP Server

The MCP server provides the following tools and resources:

#### Tools

- `list_tables`: List all tables in the catalog or a specific namespace
- `scan_table`: Scan a table with optional filters
- `table_history`: Get snapshot history of a table

#### Resources

- `table:///{namespace}/{table}`: Get metadata for a specific table

### Example Usage

Here's an example of how to use the MCP server with an MCP client:

```python
from mcp.client import MCPClient

# Connect to the MCP server
client = MCPClient("http://localhost:8000")

# List tables in the default namespace
tables = client.use_tool("list_tables", {"namespace": "default"})
print(f"Tables: {tables}")

# Scan a table with filters
data = client.use_tool("scan_table", {
    "namespace": "default",
    "table": "my_table",
    "filters": [{"field": "id", "value": 1}],
    "limit": 10
})
print(f"Data: {data}")

# Get table history
history = client.use_tool("table_history", {
    "namespace": "default",
    "table": "my_table"
})
print(f"History: {history}")

# Get table metadata
metadata = client.access_resource("table:///default/my_table")
print(f"Metadata: {metadata}")
```

## Development

### Running Tests

```bash
pytest
```

To run tests with coverage:

```bash
pytest --cov=src
```

### Project Structure

- `src/`: Source code
  - `__init__.py`: Package initialization
  - `catalog.py`: Iceberg catalog wrapper
  - `main.py`: Entry point for the application
  - `server.py`: MCP server implementation
  - `tools/`: MCP tools
    - `table_tools.py`: Tools for working with Iceberg tables
- `tests/`: Tests
  - `conftest.py`: Pytest fixtures
  - `test_catalog.py`: Tests for the catalog wrapper
  - `test_main.py`: Tests for the main module
  - `test_server.py`: Tests for the MCP server
  - `test_table_tools.py`: Tests for the table tools

## License

This project is licensed under the MIT License - see the LICENSE file for details.
