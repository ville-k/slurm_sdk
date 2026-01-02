# syntax=docker/dockerfile:1

FROM nvcr.io/nvidia/pytorch:24.06-py3

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/slurm

# Install minimal build dependencies that are typically required when
# installing the SDK and its transitive requirements.
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential git \
    && rm -rf /var/lib/apt/lists/*

# Copy the project into the build context.
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --upgrade pip \
    && pip install .

CMD ["python3", "-m", "slurm", "--help"]

