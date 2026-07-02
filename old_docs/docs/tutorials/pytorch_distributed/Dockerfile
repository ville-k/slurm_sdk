FROM python:3.12-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.8.23 /uv /uvx /usr/local/bin/

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

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

CMD ["uv", "run", "python", "train.py"]
