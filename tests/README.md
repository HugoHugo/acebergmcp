# Apache Iceberg MCP Server Tests

This directory contains integration tests for the Apache Iceberg MCP server. The tests are designed to verify that the server functions correctly and to document the expected behavior of each component.

## Test Structure

The test suite is organized as follows:

- `conftest.py`: Contains pytest fixtures for mocking the Iceberg catalog and tables
- `test_catalog.py`: Tests for the `IcebergCatalogWrapper` class
- `test_server.py`: Tests for the MCP server tools and resources
- `test_table_tools.py`: Tests for the table tools
- `test_main.py`: Tests for the main module initialization

## Running the Tests

To run the tests, use the following command from the project root:

```bash
pytest tests/
```

To run a specific test file:

```bash
pytest tests/test_server.py
```

To run a specific test:

```bash
pytest tests/test_server.py::TestMCPServer::test_list_tables_all
```

To run tests with verbose output:

```bash
pytest -v tests/
```

To run tests with coverage:

```bash
pytest --cov=src tests/
```

## Test Coverage

The tests cover the following functionality:

### Catalog Wrapper

- Initializing with default and custom configurations
- Listing namespaces
- Listing tables
- Loading tables

### MCP Server

- Listing tables across all namespaces
- Listing tables in a specific namespace
- Getting table metadata

### Table Tools

- Scanning tables without filters
- Scanning tables with filters
- Scanning tables with a custom limit
- Getting table history

### Main Module

- Initializing with default configuration
- Initializing with custom configuration
- Handling missing environment variables

## Mocking Strategy

The tests use pytest fixtures to mock the Iceberg catalog and tables. This allows the tests to run without requiring a real Iceberg catalog or tables. The mocks are configured to return realistic data that mimics the behavior of a real Iceberg catalog and tables.

The `conftest.py` file contains the following fixtures:

- `mock_catalog`: A mock Iceberg catalog
- `mock_table`: A mock Iceberg table
- `mock_catalog_wrapper`: A mock IcebergCatalogWrapper
- `setup_mcp_server`: A fixture that sets up the MCP server with the mock catalog wrapper

These fixtures are used by the tests to isolate the code under test from external dependencies.
