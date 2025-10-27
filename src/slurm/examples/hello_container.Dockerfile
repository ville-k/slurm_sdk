FROM python:3.14-alpine

WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .