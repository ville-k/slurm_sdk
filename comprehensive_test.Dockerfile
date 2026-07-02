FROM python:3.11-slim

WORKDIR /workspace
COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/
COPY tests/__init__.py tests/__init__.py
COPY tests/integration/__init__.py tests/integration/__init__.py
COPY tests/integration/container_test_tasks.py tests/integration/

RUN pip install --no-cache-dir .

# Tests module isn't installed as a package, so add to PYTHONPATH
ENV PYTHONPATH=/workspace:$PYTHONPATH
