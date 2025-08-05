# Use official Python base image
FROM python:3.13.5-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src

# Set entrypoint
CMD ["python", "src/main.py"]
