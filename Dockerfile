# Use official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY process_contracts.py .
COPY search_contracts.py .

# Create logs directory
RUN mkdir -p /app/logs

# Set environment variables
ENV STORAGE_CONNECTION_STRING=""
ENV AZURE_OPENAI_KEY=""
ENV AZURE_OPENAI_ENDPOINT=""
ENV SEARCH_ENDPOINT=""
ENV SEARCH_KEY=""

# Set Python to unbuffered mode
ENV PYTHONUNBUFFERED=1

# Run the processing script
CMD ["python", "-u", "process_contracts.py"]