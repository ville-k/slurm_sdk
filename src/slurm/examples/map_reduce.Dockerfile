FROM python:3.11-slim

# Set working directory
WORKDIR /workspace

# Copy project files
COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/

# Install the slurm-sdk package
RUN pip install --no-cache-dir .

# Default command (will be overridden by slurm runner)
CMD ["python"]
