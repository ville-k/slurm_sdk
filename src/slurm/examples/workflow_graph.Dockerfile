# Minimal Python 3.11 container for workflow graph visualization example
FROM python:3.11-slim

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy and install the slurm SDK
COPY . /app
RUN uv pip install --system --no-cache-dir /app

# Set Python path
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["/bin/bash"]
