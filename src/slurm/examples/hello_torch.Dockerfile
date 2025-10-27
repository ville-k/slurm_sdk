# ---- Stage 1: Dependency build layer (changes rarely) ----
FROM python:3.14-slim AS deps

WORKDIR /workspace

# Copy dependency manifests only (so this layer is cached)
COPY pyproject.toml README.md ./
COPY src/ src/

# Install project dependencies without bringing in your src code
# This ensures caching unless pyproject.toml changes
RUN pip install --no-cache-dir .

# Optionally preinstall torch to isolate it from frequent rebuilds
RUN pip install --no-cache-dir torch

# ---- Stage 2: Runtime layer (changes frequently) ----
FROM python:3.14-slim AS runtime

WORKDIR /workspace

# Copy Python packages from deps stage
COPY --from=deps /usr/local/lib/python3.14 /usr/local/lib/python3.14
COPY --from=deps /usr/local/bin /usr/local/bin

# Copy only your code (this invalidates cache when src changes)
COPY src/ src/
