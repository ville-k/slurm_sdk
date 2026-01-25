FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /workspace

COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/

# Note: Avoid 'pip install --upgrade pip' as it creates AUFS whiteouts that
# enroot cannot convert in nested container environments (e.g., podman-in-podman)
RUN pip install .

CMD ["python3", "-m", "slurm", "--help"]
