FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /workspace

# Install minimal build dependencies
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential git \
    && rm -rf /var/lib/apt/lists/*

# Copy the project into the build context
COPY pyproject.toml README.md ./
COPY src/ src/

# Note: Avoid 'pip install --upgrade pip' as it creates AUFS whiteouts that
# enroot cannot convert in nested container environments (e.g., podman-in-podman)
RUN pip install .

CMD ["python3", "-m", "slurm", "--help"]
