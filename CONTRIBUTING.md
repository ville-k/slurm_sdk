# Contributing

Thanks for contributing to the Python SLURM SDK!

## Setup

- Install uv (see `https://github.com/astral-sh/uv`)
- Run tests:

```
uv run pytest
```

## Testing

- Unit tests are offline and use the local backend; add tests under `tests/`.
- SSH integration tests are optional; prefer marking them and enabling via env vars (e.g., `SLURM_HOST`, `SLURM_USER`).
- Use the `LocalBackend` for quick end-to-end checks without external services.

## Style

- Follow the Google Python Style Guide for public API docstrings and type hints.
- Keep info-level logs concise; prefer debug level for detailed internals.
- Use `slurm.logging.configure_logging()` for a pleasant default logging setup.

## Submitting Changes

- Ensure `uv run pytest` passes.
- Update docs (`README.md`, examples) when adding features that affect public APIs.
