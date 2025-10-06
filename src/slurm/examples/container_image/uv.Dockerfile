# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install minimal build dependencies that are typically required when
# installing the SDK and its transitive requirements.
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv and uvx to /bin directory - this ensures they are in the PATH
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/

# Set working directory first, before adding files
WORKDIR /app

# Add project files
ADD . /app

# Install the project into /opt/venv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

# Sync dependencies - creates the virtual environment at /opt/venv
RUN uv sync --locked

# Activate the virtual environment by setting VIRTUAL_ENV and updating PATH
# VIRTUAL_ENV tells uv run to use this environment
# PATH ensures executables from the venv are found first
ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

CMD ["uv", "run", "python", "-m", "slurm", "--help"]
