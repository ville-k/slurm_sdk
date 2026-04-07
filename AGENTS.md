# Repository Guidelines

## Project Structure & Module Organization

- `src/slurm/` hosts the core SDK: job orchestration (`cluster.py`, `job.py`), decorators (`task.py`), packaging utilities, and renderers. Keep new modules under this package with explicit exports in `__init__.py`.
- `src/slurm/examples/` contains runnable usage samples; mirror this pattern when adding new tutorials.
- `tests/` holds pytest suites with shared fixtures in `tests/conftest.py` and helpers under `tests/helpers/`.
- `docs/` and `mkdocs.yml` drive the MkDocs site; place new guides under `docs/` and add navigation entries in `mkdocs.yml`.
- Project configuration lives at the repository root (`pyproject.toml`, `uv.lock`, `README.md`).

## Build, Test, and Development Commands

All commands are run through `uv run`, which automatically syncs the project environment (installs/updates dependencies from the lockfile) before execution. No manual install step is needed.

- `uv run pytest` executes the offline unit suite against the local backend.
- `uv run pytest -n auto` runs tests in parallel using all available CPU cores (via pytest-xdist).
- `uv run ty check` runs the ty type checker against `src/`.
- `uv run python -m slurm.examples.hello_world` performs a smoke test of job submission without SLURM access.
- `uv run mkdocs serve` launches the documentation preview at `http://127.0.0.1:8000`; stop with `Ctrl+C`.
- `uv run mkdocs build` builds the documentation and checks for warnings and errors.
- `uv format` formats code according to project style guidelines.
- `uv run ruff check --fix` lints the code and auto-fixes issues where possible.
- `uv run bandit -r src/ -ll` runs security analysis; fails on HIGH or MEDIUM severity issues. Run before submitting PRs.
- `uv run mdformat docs/tutorials docs/how-to docs/explanation docs/reference docs/CHANGELOG.md docs/CONTRIBUTING.md README.md AGENTS.md` formats markdown files (note: `docs/index.md` is excluded as it uses special MkDocs Material grid syntax).
- `uv run mdformat --check docs/tutorials docs/how-to docs/explanation docs/reference docs/CHANGELOG.md docs/CONTRIBUTING.md README.md AGENTS.md` checks markdown formatting without modifying files.

## Agent Development Workflow

When implementing a new feature, follow this workflow:

### 1. Plan the Work

Before writing code, understand the scope and design:

- Read relevant existing code to understand patterns and conventions
- Identify which modules need changes
- Consider edge cases and error handling
- Break complex features into smaller, testable increments

### 2. Develop with Tests

Write code and tests together, maintaining high coverage:

- Write tests alongside implementation, not after
- Run `uv run pytest` frequently to catch regressions early
- Aim for comprehensive test coverage of new functionality
- Use `uv run pytest -n auto` for faster parallel test execution
- Keep the test suite passing at all times

### 3. Add Documentation and Changelog

When coding is complete:

- Update or add documentation following the Diataxis framework (invoke `/docs` for detailed guidance)
- Add changelog entries to `docs/CHANGELOG.md` under `## [Unreleased]`
- Ensure docstrings are complete for public APIs

### 4. Lint and Validate

Before committing, run all quality checks:

```bash
uv format
uv run ruff check --fix
uv run ty check
uv run bandit -r src/ -ll
uv run mdformat docs/tutorials docs/how-to docs/explanation docs/reference docs/CHANGELOG.md docs/CONTRIBUTING.md README.md AGENTS.md
uv run mkdocs build
```

Fix any errors or warnings before proceeding.

### 5. Commit Changes

Create a commit following Conventional Commits:

- Use appropriate type: `feat`, `fix`, `docs`, `refactor`, `test`, etc.
- Write a clear, concise description
- Include body text explaining "why" for non-trivial changes
- Reference related issues if applicable

### 6. Create Pull Request

**Once all completion criteria are met, immediately create a PR for review:**

```bash
git push -u origin <branch-name>
gh pr create --fill
```

- Never push directly to `main` - all changes require code review
- Push and create PR as soon as tests pass and linting is clean
- The PR description should summarize changes and reference any related issues
- Wait for CI to pass before requesting human review

## Publishing to PyPI

The package is published to PyPI via GitHub Actions using trusted publishing (no API tokens needed).

### Dev Releases

Dev releases publish the current version in `pyproject.toml` (e.g., `0.4.5-dev`) for testing:

```bash
gh workflow run publish.yml -f version_type=dev
```

To test the build without uploading:

```bash
gh workflow run publish.yml -f version_type=dev -f dry_run=true
```

### Production Releases

Production releases require a clean version number and updated changelog:

1. **Update version** in `pyproject.toml` (remove `-dev` suffix):

   ```python
   version = "0.4.5"  # was "0.4.5-dev"
   ```

1. **Update changelog** in `docs/CHANGELOG.md`:

   - Move entries from `## [Unreleased]` to new section `## [0.4.5] - YYYY-MM-DD`
   - Keep an empty `## [Unreleased]` section at the top

1. **Commit, tag, and create GitHub release**:

   ```bash
   git add pyproject.toml docs/CHANGELOG.md
   git commit -m "chore: release v0.4.5"
   git tag v0.4.5
   git push origin main --tags
   gh release create v0.4.5 --generate-notes
   ```

   The GitHub release event automatically triggers PyPI publishing.

1. **Prepare for next development cycle**:

   ```bash
   # Update version to next dev version
   # version = "0.4.6-dev"
   git commit -am "chore: bump version to 0.4.6-dev"
   git push
   ```

### Manual Production Release

If you need to publish a release without creating a GitHub release:

```bash
gh workflow run publish.yml -f version_type=release
```

This validates that the version doesn't contain `-dev`, `-alpha`, or `-beta` suffixes.

## Coding Style & Naming Conventions

- Use 4-space indentation and type hints throughout; the package ships `py.typed`.
- Follow Google-style docstrings for public APIs and mirror existing logging patterns (`slurm.logging.configure_logging()`).
- Prefer snake_case for functions, PascalCase for classes, and keep module names lowercase.
- Avoid restructuring logs: reserve INFO for user-facing messaging and DEBUG for internals.

## Code Comments Guidelines

### Prefer "Why" Over "What"

Code should be self-documenting through clear naming and structure. Comments should explain **why** decisions were made, not **what** the code does.

❌ **Bad (what-style)**:

```python
# Increment counter by 1
counter += 1

# Loop through users
for user in users:
    # Check if user is active
    if user.is_active:
```

✅ **Good (why-style)**:

```python
# Increment before check to avoid off-by-one error in batch processing
counter += 1

# Process only active users to prevent sending notifications to deactivated accounts
for user in users:
    if user.is_active:
```

### When to Use What-Style Comments

Use what-style comments **only** when code is necessarily complex or unintuitive:

- **Non-obvious algorithms**: `# Binary search to achieve O(log n) lookup`
- **Performance optimizations**: `# Cache miss forces full table scan here`
- **Domain-specific logic**: `# SEC regulation requires T+2 settlement`
- **Working around limitations**: `# PyTorch autograd doesn't support in-place ops here`
- **Complex mathematical operations**: `# Haversine formula for great-circle distance`
- **Regex patterns**: `# Match ISO 8601 datetime with optional timezone`

### Best Practices

- **Delete obvious comments**: If the code is clear, no comment is better than a redundant one
- **Explain decisions**: Why this approach over alternatives? What tradeoffs were made?
- **Document assumptions**: What must be true for this code to work correctly?
- **Flag technical debt**: `# TODO: Refactor when API v2 launches` with context
- **Keep comments up to date**: Outdated comments are worse than no comments

**Rule of thumb**: If you can make the code clearer instead of adding a comment, refactor the code.

## Changelog Management

When modifying code, always update `docs/CHANGELOG.md` following the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format:

### Format Requirements

- **File**: `docs/CHANGELOG.md`
- **Date format**: ISO 8601 (YYYY-MM-DD)
- **Structure**: Reverse chronological (newest first)
- **Semantic versioning**: Link to [semver.org](https://semver.org/spec/v2.0.0.html)

### Entry Categories

Classify all changes under these headings:

- **Added**: New features
- **Changed**: Changes in existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Now removed features
- **Fixed**: Bug fixes
- **Security**: Vulnerability fixes

### Workflow

1. **During development**: Add entries to `## [Unreleased]` section
1. **At release**: Move `[Unreleased]` entries to new versioned section `## [X.Y.Z] - YYYY-MM-DD`
1. **Format**: Use bullet points (`-`) with descriptive, user-focused language
1. **Audience**: Write for end users, not developers - explain _what_ and _why_, not implementation details

### Example Structure

```markdown
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New API endpoint for batch processing requests
- Support for concurrent job execution with configurable worker pools

### Fixed
- Memory leak in long-running data pipeline operations

## [1.2.0] - 2025-01-15

### Added
- REST API for job management
- Webhook notifications on job completion

### Changed
- Improved error messages with actionable resolution steps
- Database connection pooling now uses exponential backoff

### Deprecated
- Legacy `/v1/process` endpoint (use `/v2/jobs` instead)

## [1.1.0] - 2025-01-01

### Added
- Initial release with core processing capabilities
```

### Key Principles

- ✅ **Do**: Focus on user-facing changes and their impact
- ✅ **Do**: Group similar changes together under appropriate categories
- ✅ **Do**: Keep entries concise but descriptive
- ❌ **Don't**: Include every commit or minor internal refactoring
- ❌ **Don't**: Use commit messages as changelog entries
- ❌ **Don't**: Forget to mention breaking changes or deprecations

## Type Checking

Types are checked using [ty](https://docs.astral.sh/ty/), configured in `pyproject.toml` under `[tool.ty]`.

- `uv run ty check` runs the type checker against `src/` (as configured in `[tool.ty.src]`).
- `uv run ty check --watch` watches for file changes and rechecks incrementally.
- `uv run ty explain <rule>` explains a specific diagnostic rule.

The project uses gradual adoption: noisy rules (e.g., `unresolved-attribute`, `invalid-type-form`) are set to `"warn"` and should be promoted to `"error"` as violations are fixed. New code should not introduce new type errors or warnings.

## Testing Guidelines

- Base tests on `pytest`; name files `test_*.py` and co-locate fixtures or builders under `tests/helpers/`.
- Cover new behaviors with local-backend tests; mock SSH interactions unless explicitly targeting integration scenarios.
- Mark slower or external tests clearly (e.g., `pytest.mark.ssh`) and keep them skipped by default.
- Run `uv run pytest` before opening a PR and document any deviations.

## Error Handling

- Define custom exception classes for domain-specific errors; inherit from a common project base exception to allow callers to catch broadly when appropriate.
- Let unexpected errors bubble up — don't catch broad `Exception` unless logging and re-raising. Silent swallowing masks bugs.
- Validate at system boundaries (user input, API requests, external data); trust internal code and framework guarantees within the core.
- Use structured error messages with actionable context: include what failed, why, and what the user can do about it.
- Log errors at the appropriate level: `WARNING` for recoverable issues, `ERROR` for failures that need attention, `CRITICAL` for system-level failures.

## Dependency Management

Dependencies are managed entirely through UV. Never use `pip install` directly.

- `uv add <package>` adds a dependency to `pyproject.toml` and updates `uv.lock`.
- `uv add --dev <package>` adds a development-only dependency.
- `uv remove <package>` removes a dependency.
- `uv sync` syncs the environment to match the lockfile without running a command.
- `uv lock --upgrade-package <package>` upgrades a specific package within its version constraints.
- `uv tree` displays the project's dependency tree.

## Commit & Pull Request Guidelines

- Commit messages follow concise sentence-case summaries (see `git log`), optionally followed by descriptive body text.
- Reference issues when applicable and record the motivation for API changes.
- Include testing evidence (command + result) in PR descriptions and update docs or examples when behavior changes.
- Provide screenshots or terminal output for documentation-facing adjustments.

## Git Commit Message Guidelines

Follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for clear, machine-readable commit messages.

### Format

```text
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Required Types

- **feat**: New feature (correlates with MINOR version bump)
- **fix**: Bug fix (correlates with PATCH version bump)

### Common Additional Types

- **docs**: Documentation changes
- **refactor**: Code refactoring without feature/fix
- **perf**: Performance improvements
- **test**: Adding or updating tests
- **build**: Build system or dependency changes
- **ci**: CI/CD configuration changes
- **chore**: Maintenance tasks

### Breaking Changes

Indicate with `!` after type/scope OR with `BREAKING CHANGE:` footer (correlates with MAJOR version bump):

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Scope (Optional)

Add scope in parentheses for context:

```text
feat(api): add rate limiting
fix(auth): resolve token refresh race condition
docs(readme): update installation instructions
```

### Examples

**Simple fix:**

```text
fix: prevent race condition in request handling
```

**Feature with scope:**

```text
feat(training): add gradient checkpointing for memory efficiency
```

**Breaking change:**

```text
feat(api)!: change authentication flow to OAuth2

BREAKING CHANGE: API now requires OAuth2 tokens instead of API keys.
Migration guide: https://docs.example.com/oauth2-migration
```

**Multi-paragraph body:**

```text
fix: resolve distributed training hang on GB200

Introduce request ID tracking and dismiss responses from stale requests.
This prevents the race condition where concurrent requests would deadlock
the training loop.

Remove timeout workarounds which are now obsolete.

Refs: #1234
```

### Best Practices

- **Use imperative mood**: "add feature" not "added feature" or "adds feature"
- **Lowercase types**: `feat:` not `FEAT:`
- **No period at end**: Description should not end with `.`
- **Keep description under 72 characters**: Forces conciseness
- **Use body for "why"**: Explain motivation, context, and tradeoffs
- **Reference issues**: Use `Refs: #123` or `Fixes: #456` in footer

### Benefits

- Automated changelog generation
- Automated semantic versioning
- Clear communication of change nature
- Easier navigation of git history
- Structured history for tooling

## Documentation Management

Documentation follows the [Diataxis](https://diataxis.fr/) framework with four types: tutorials, how-to guides, reference, and explanation. **Never mix documentation types** — each serves a fundamentally different user need.

When writing or updating documentation, invoke the `/docs` skill for detailed templates, decision guides, and common mistakes to avoid.

Key rules:

- All public and private APIs have Google-style docstrings.
- Documentation is published using Material for MkDocs.
- Use [Mermaid.js](https://mermaid.js.org/) diagrams where they clarify complex concepts.
- Add navigation entries in `mkdocs.yml` for new pages.
- `docs/index.md` is excluded from mdformat (uses special MkDocs Material grid syntax).

## Security & Configuration Tips

- Store SSH credentials via environment variables or your SSH config; never commit secrets.
- Validate remote cluster settings in a private `.env` file and document required variables in PR discussions.
