FROM python:3.14-alpine

WORKDIR /workspace
COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/

RUN pip install --no-cache-dir .