FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY .env ./

# Create logs directory
RUN mkdir -p logs

# Expose port for MCP server
EXPOSE 8000

# Run the MCP server
CMD ["python", "-m", "src.main"]
