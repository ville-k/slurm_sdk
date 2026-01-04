# Repository Guidelines

## Project Structure & Module Organization
- `src/slurm/` hosts the core SDK: job orchestration (`cluster.py`, `job.py`), decorators (`task.py`), packaging utilities, and renderers. Keep new modules under this package with explicit exports in `__init__.py`.
- `src/slurm/examples/` contains runnable usage samples; mirror this pattern when adding new tutorials.
- `tests/` holds pytest suites with shared fixtures in `tests/conftest.py` and helpers under `tests/helpers/`.
- `docs/` and `mkdocs.yml` drive the MkDocs site; place new guides under `docs/` and add navigation entries in `mkdocs.yml`.
- Project configuration lives at the repository root (`pyproject.toml`, `uv.lock`, `README.md`).

## Build, Test, and Development Commands
- `uv pip install -e .` installs the package in editable mode for local development.
- `uv run pytest` executes the offline unit suite against the local backend.
- `uv run python -m slurm.examples.hello_world` performs a smoke test of job submission without SLURM access.
- `uv run mkdocs serve` launches the documentation preview at `http://127.0.0.1:8000`; stop with `Ctrl+C`.

## Coding Style & Naming Conventions
- Use 4-space indentation and type hints throughout; the package ships `py.typed`.
- Follow Google-style docstrings for public APIs and mirror existing logging patterns (`slurm.logging.configure_logging()`).
- Prefer snake_case for functions, PascalCase for classes, and keep module names lowercase.
- Avoid restructuring logs: reserve INFO for user-facing messaging and DEBUG for internals.

## Testing Guidelines
- Base tests on `pytest`; name files `test_*.py` and co-locate fixtures or builders under `tests/helpers/`.
- Cover new behaviors with local-backend tests; mock SSH interactions unless explicitly targeting integration scenarios.
- Mark slower or external tests clearly (e.g., `pytest.mark.ssh`) and keep them skipped by default.
- Run `uv run pytest` before opening a PR and document any deviations.

## Commit & Pull Request Guidelines
- Commit messages follow concise sentence-case summaries (see `git log`), optionally followed by descriptive body text.
- Reference issues when applicable and record the motivation for API changes.
- Include testing evidence (command + result) in PR descriptions and update docs or examples when behavior changes.
- Provide screenshots or terminal output for documentation-facing adjustments.

## Security & Configuration Tips
- Store SSH credentials via environment variables or your SSH config; never commit secrets.
- Validate remote cluster settings in a private `.env` file and document required variables in PR discussions.
