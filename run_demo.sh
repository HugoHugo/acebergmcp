#!/bin/bash
# Script to run the Iceberg MCP Server demo

# Set colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Iceberg MCP Server Demo${NC}"
echo -e "${YELLOW}This will start the following services:${NC}"
echo "- MinIO (S3 storage)"
echo "- Iceberg REST Catalog"
echo "- Sample table creation and data insertion"
echo "- Iceberg MCP Server"
echo "- Demo client"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker and try again.${NC}"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Pull required images first to avoid issues
echo -e "${YELLOW}Pulling required Docker images...${NC}"
docker pull minio/minio
docker pull minio/mc
docker pull tabulario/iceberg-rest:0.7.0

echo -e "${GREEN}Starting Docker Compose services...${NC}"
echo -e "${YELLOW}This may take a few minutes for the first run as images are built.${NC}"

# Start the services in detached mode
docker compose -f docker-compose.demo.yml up --build -d

# Check if the services are running
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to start Docker Compose services. Please check the logs for more information.${NC}"
    exit 1
fi

echo -e "${GREEN}Services started successfully!${NC}"
echo -e "${YELLOW}Waiting for services to initialize...${NC}"

# Follow the logs of the demo client to see the output
echo -e "${GREEN}Showing logs from the demo client:${NC}"
docker compose -f docker-compose.demo.yml logs -f demo-client

# Check if the demo client completed successfully
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Demo completed successfully!${NC}"
    echo -e "${YELLOW}You can access the following services:${NC}"
    echo "- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    echo "- Iceberg REST Catalog: http://localhost:8181"
    echo "- Iceberg MCP Server: http://localhost:8000"
    echo ""
    echo -e "${YELLOW}To stop the services, run:${NC}"
    echo "docker compose -f docker-compose.demo.yml down"
else
    echo -e "${RED}Demo failed. Please check the logs for more information.${NC}"
    echo -e "${YELLOW}You can view the logs with:${NC}"
    echo "docker compose -f docker-compose.demo.yml logs"
fi
