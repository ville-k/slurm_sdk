# Development Guidelines

## Architecture

Software architecture should follow the Functional Core, Imperative Shell (FCIS) pattern to structure software by separating complex, deterministic business logic (the pure functional core) from side-effect-heavy orchestration (the imperative shell). The core handles data manipulation, while the shell manages I/O, database access, and external systems, making the codebase highly testable and robust.

We prefer modern fluent object oriented Python code while striving to use functional style programming patterns where it naturally fits with the language. The implementation of object oriented components follows SOLID principles.

Services should avoid being small micro services just for the sake of decomposition. Instead we should strive to encapsulate enough functionality to form a coherent product identity for a service while keeping the development and deployment of it agile. We prefer exposing service functionality through REST APIs that expose schemas through the [JSON schema](https://json-schema.org/) standard.

Services should usually be accompanied by Python SDKs and CLIs that expose their functionality. CLI applications should expose functionality using subcommands that clearly map to library and REST API structure and concepts to make navigation between them easy.

Public API design for library / SDK APIs should follow the principle of progressive disclosure, as advocated by Fran&ccedil;ois Chollet.


## Libraries Used

We strive to keep the set of core dependencies small.

- Database access: SQLModel
- Data models for business objects and configuration: Pydantic
- Services written using FastAPI
- Command line applications use [cyclopts](https://cyclopts.readthedocs.io/en/stable/)
- Web UIs use the [nicegui](https://nicegui.io/) library
- Unit and integration tests use [pytest](https://docs.pytest.org/en/stable/)
- UV for managing the workspace and dependencies
- Agents and AI features leverage [Pydantic AI](https://ai.pydantic.dev/) library
- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) for publishing documentation


## Project Structure

- Source code lives under `src/<package>/` with explicit exports in `__init__.py`.
- `src/<package>/examples/` contains runnable usage samples; mirror this pattern when adding new tutorials.
- `tests/` holds pytest suites with shared fixtures in `tests/conftest.py` and helpers under `tests/helpers/`.
- `docs/` and `mkdocs.yml` drive the MkDocs site; place new guides under `docs/` and add navigation entries in `mkdocs.yml`.
- Project configuration lives at the repository root (`pyproject.toml`, `uv.lock`, `README.md`).


## Build, Test, and Development Commands

All commands are run through `uv run`, which automatically syncs the project environment (installs/updates dependencies from the lockfile) before execution. No manual install step is needed.

- `uv run pytest` executes the offline unit suite against the local backend.
- `uv run pytest -n auto` runs tests in parallel using all available CPU cores (via pytest-xdist).
- `uv run mkdocs serve` launches the documentation preview at `http://127.0.0.1:8000`; stop with `Ctrl+C`.
- `uv run mkdocs build` builds the documentation and checks for warnings and errors.
- `uv format` formats code according to project style guidelines.
- `uv run ruff check --fix` lints the code and auto-fixes issues where possible.
- `uv run bandit -r src/ -ll` runs security analysis; fails on HIGH or MEDIUM severity issues. Run before submitting PRs.
- `uv run mdformat <docs-paths>` formats markdown documentation files. Exact paths to include/exclude are project-specific; define them in the project's `AGENTS.md`.


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

- Update or add documentation following the [Documentation Guide](DOCUMENTATION_GUIDE.md)
- Add changelog entries to `docs/CHANGELOG.md` under `## [Unreleased]`
- Ensure docstrings are complete for public APIs

### 4. Lint and Validate

Before committing, run all quality checks:

```bash
uv format
uv run ruff check --fix
uv run bandit -r src/ -ll
uv run mdformat <docs-paths>
uv run mkdocs build
```

Fix any errors or warnings before proceeding.

### 5. Commit Changes

Create a commit following [Conventional Commits](#git-commit-message-guidelines):

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

- Never push directly to `main` — all changes require code review
- Push and create PR as soon as tests pass and linting is clean
- The PR description should summarize changes and reference any related issues
- Wait for CI to pass before requesting human review


## Code Style

Follow [Google Python Styleguide](https://google.github.io/styleguide/pyguide.html).

### Naming Conventions

- Use 4-space indentation and type hints throughout; packages ship `py.typed`.
- Follow Google-style docstrings for all public APIs.
- Prefer snake_case for functions, PascalCase for classes, and keep module names lowercase.
- Reserve INFO logging for user-facing messages and DEBUG for internals.

### Code Comments

Code should be self-documenting through clear naming and structure. Comments should explain **why** decisions were made, not **what** the code does.

Bad (what-style):

```python
# Increment counter by 1
counter += 1

# Loop through users
for user in users:
    # Check if user is active
    if user.is_active:
```

Good (why-style):

```python
# Increment before check to avoid off-by-one error in batch processing
counter += 1

# Process only active users to prevent sending notifications to deactivated accounts
for user in users:
    if user.is_active:
```

#### When What-Style Comments Are Appropriate

Use what-style comments **only** when code is necessarily complex or unintuitive:

- **Non-obvious algorithms**: `# Binary search to achieve O(log n) lookup`
- **Performance optimizations**: `# Cache miss forces full table scan here`
- **Domain-specific logic**: `# SEC regulation requires T+2 settlement`
- **Working around limitations**: `# PyTorch autograd doesn't support in-place ops here`
- **Complex mathematical operations**: `# Haversine formula for great-circle distance`
- **Regex patterns**: `# Match ISO 8601 datetime with optional timezone`

#### Comment Best Practices

- **Delete obvious comments**: If the code is clear, no comment is better than a redundant one
- **Explain decisions**: Why this approach over alternatives? What tradeoffs were made?
- **Document assumptions**: What must be true for this code to work correctly?
- **Flag technical debt**: `# TODO: Refactor when API v2 launches` with context
- **Keep comments up to date**: Outdated comments are worse than no comments

**Rule of thumb**: If you can make the code clearer instead of adding a comment, refactor the code.


## Type Checking

Code uses type annotations at all interfaces. Types are checked using [ty](https://docs.astral.sh/ty/), an extremely fast Python type checker written in Rust (by the same team behind UV and Ruff).

### Running ty

- `uv run ty check` checks all Python files in the project.
- `uv run ty check src/` checks a specific directory.
- `uv run ty check --watch` watches for file changes and rechecks incrementally.
- `uv run ty explain <rule>` explains a specific diagnostic rule.

### Configuration

Configure ty in `pyproject.toml` under `[tool.ty]`:

```toml
[tool.ty.environment]
python-version = "3.12"

[tool.ty.src]
include = ["src", "tests"]

[tool.ty.rules]
possibly-unresolved-reference = "warn"

# Relax rules for test files
[[tool.ty.overrides]]
include = ["tests/**"]

[tool.ty.overrides.rules]
possibly-unresolved-reference = "ignore"
```

Key configuration sections:

- **`[tool.ty.rules]`** — set individual rules to `"error"`, `"warn"`, or `"ignore"`.
- **`[tool.ty.environment]`** — set `python-version`, `python-platform`, and search `root` paths.
- **`[tool.ty.src]`** — control which files to `include`/`exclude`.
- **`[[tool.ty.overrides]]`** — apply different rules to specific file patterns (e.g., relaxing strictness in tests).

### CI Integration

Use `--output-format github` or `--output-format gitlab` for inline annotations in CI. Use `--error-on-warning` to fail the build on warnings.


## Testing

All code tests are written using pytest and live in a `tests/` directory whose internal structure mirrors the source code layout.

- Name test files `test_*.py` and co-locate fixtures or builders under `tests/helpers/`.
- Cover new behaviors with local-backend tests; mock external interactions unless explicitly targeting integration scenarios.
- Mark slower or external tests clearly (e.g., `pytest.mark.integration`) and keep them skipped by default.
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
- `uv add --dev <package>` adds a development-only dependency (via PEP 735 dependency groups).
- `uv add --group <name> <package>` adds to a named dependency group (e.g., `--group lint`, `--group test`).
- `uv remove <package>` removes a dependency from `pyproject.toml` and the lockfile.
- `uv sync` syncs the environment to match the lockfile without running a command.
- `uv lock --upgrade-package <package>` upgrades a specific package within its version constraints.
- `uv tree` displays the project's dependency tree.

### Dependency Principles

- Keep the set of core dependencies small. Every new dependency is a maintenance and security liability.
- Before adding a dependency, evaluate: is it well-maintained? Does it have a compatible license? Could we achieve the same with a small amount of code?
- Keep `pyproject.toml` version constraints flexible (e.g., `>=1.0,<2`) and let the lockfile (`uv.lock`) pin exact versions for reproducibility.
- Run `uv audit` and `uv run bandit -r src/ -ll` regularly to check for known vulnerabilities.
- When upgrading dependencies, run the full test suite and check for deprecation warnings.


## Change and Release Management

Versioning of software and data schemas uses [Semantic Versioning](https://semver.org/).

All changes are documented in `docs/CHANGELOG.md` using [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

### Changelog Format

- **Date format**: ISO 8601 (YYYY-MM-DD)
- **Structure**: Reverse chronological (newest first)

Classify all changes under these headings:

- **Added**: New features
- **Changed**: Changes in existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Now removed features
- **Fixed**: Bug fixes
- **Security**: Vulnerability fixes

### Changelog Workflow

1. **During development**: Add entries to `## [Unreleased]` section
2. **At release**: Move `[Unreleased]` entries to new versioned section `## [X.Y.Z] - YYYY-MM-DD`
3. **Format**: Use bullet points (`-`) with descriptive, user-focused language
4. **Audience**: Write for end users, not developers — explain _what_ and _why_, not implementation details

### Changelog Example

```markdown
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New API endpoint for batch processing requests

### Fixed
- Memory leak in long-running data pipeline operations

## [1.2.0] - 2025-01-15

### Added
- REST API for job management

### Changed
- Improved error messages with actionable resolution steps

### Deprecated
- Legacy `/v1/process` endpoint (use `/v2/jobs` instead)
```

### Changelog Principles

- Focus on user-facing changes and their impact
- Group similar changes together under appropriate categories
- Don't include every commit or minor internal refactoring
- Don't use commit messages as changelog entries
- Always mention breaking changes or deprecations

### Publishing to PyPI

The package is published to PyPI via GitHub Actions using trusted publishing (no API tokens needed).

#### Dev Releases

Dev releases publish the current version in `pyproject.toml` (e.g., `0.4.5-dev`) for testing:

```bash
gh workflow run publish.yml -f version_type=dev
```

To test the build without uploading:

```bash
gh workflow run publish.yml -f version_type=dev -f dry_run=true
```

#### Production Releases

Production releases require a clean version number and updated changelog:

1. **Update version** in `pyproject.toml` (remove `-dev` suffix)
2. **Update changelog** in `docs/CHANGELOG.md`: move entries from `## [Unreleased]` to new versioned section `## [X.Y.Z] - YYYY-MM-DD`; keep an empty `## [Unreleased]` section at the top
3. **Commit, tag, and create GitHub release**:

   ```bash
   git add pyproject.toml docs/CHANGELOG.md
   git commit -m "chore: release vX.Y.Z"
   git tag vX.Y.Z
   git push origin main --tags
   gh release create vX.Y.Z --generate-notes
   ```

   The GitHub release event automatically triggers PyPI publishing.

4. **Prepare for next development cycle**: bump version to next dev version and push


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

### Commit Best Practices

- **Use imperative mood**: "add feature" not "added feature" or "adds feature"
- **Lowercase types**: `feat:` not `FEAT:`
- **No period at end**: Description should not end with `.`
- **Keep description under 72 characters**: Forces conciseness
- **Use body for "why"**: Explain motivation, context, and tradeoffs
- **Reference issues**: Use `Refs: #123` or `Fixes: #456` in footer

### Pull Request Guidelines

- Reference issues when applicable and record the motivation for API changes.
- Include testing evidence (command + result) in PR descriptions and update docs or examples when behavior changes.
- Provide screenshots or terminal output for documentation-facing adjustments.


## CI/CD Pipeline

Every project should have a CI pipeline that runs on pull requests and merges to `main`. The pipeline should execute these checks in order of speed (fail fast):

1. **Format check** — `uv format --check` (seconds)
2. **Lint** — `uv run ruff check` (seconds)
3. **Type check** — `uv run ty check --error-on-warning` (seconds)
4. **Security scan** — `uv run bandit -r src/ -ll` (seconds)
5. **Unit tests** — `uv run pytest -n auto` (seconds to minutes)
6. **Markdown format check** — `uv run mdformat --check <docs-paths>` (seconds)
7. **Documentation build** — `uv run mkdocs build --strict` (seconds to minutes)

Integration tests that require external resources should run on a separate schedule or be triggered manually, not on every PR.


## Code Review

All changes go through pull request review before merging. Reviewers should check:

- **Correctness**: Does the code do what the PR claims? Are edge cases handled?
- **Tests**: Are new behaviors covered? Do tests verify the right thing (not just exercise code)?
- **API design**: Are public interfaces clear, consistent, and follow progressive disclosure?
- **Security**: No secrets committed, no injection vectors, input validated at boundaries.
- **Documentation**: Are changelog, docstrings, and user-facing docs updated?
- **Scope**: Does the PR stay focused, or does it bundle unrelated changes?

Authors should keep PRs small and focused. A PR that does one thing well is easier to review than one that does five things adequately.


## Documentation Style

Developer-facing documentation follows the [Diataxis](https://diataxis.fr/start-here/) framework. See the [Documentation Guide](DOCUMENTATION_GUIDE.md) for full details on the four documentation types (tutorials, how-to guides, reference, explanation), templates, and common mistakes.

Key principles:

- Documentation is published using Material for MkDocs.
- Documentation is written in markdown and formatted using mdformat.
- All public and private APIs have Google-style docstrings.
- Never mix documentation types — each serves a different user need.
- Use [Mermaid.js](https://mermaid.js.org/) diagrams where they clarify complex concepts.


## Security & Configuration Tips

- Store credentials via environment variables or config files; never commit secrets.
- Validate settings in a private `.env` file and document required variables in PR discussions.
- Run `uv run bandit -r src/ -ll` regularly and before every PR.
- Review dependency advisories when upgrading packages.
