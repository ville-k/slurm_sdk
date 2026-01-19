import os
import sys

import pytest


# Ensure 'src' and 'tests/helpers' are on sys.path for package imports in tests
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(TESTS_DIR, os.pardir))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
HELPERS_DIR = os.path.join(TESTS_DIR, "helpers")

# Add directories as absolute paths so they work when runner changes working directory
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
if HELPERS_DIR not in sys.path:
    sys.path.insert(0, HELPERS_DIR)
# Also add tests dir so test modules can be imported by the runner
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)


def _env_truthy(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() not in {"", "0", "false", "no"}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests that require external services (e.g. docker-compose).",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    run_integration = config.getoption("--run-integration") or _env_truthy(
        "SLURM_RUN_INTEGRATION"
    )
    if run_integration:
        return

    skip_marker = pytest.mark.skip(
        reason="Integration tests are skipped by default; pass --run-integration or set SLURM_RUN_INTEGRATION=1."
    )
    for item in items:
        if (
            "integration_test" in item.keywords
            or "slow_integration_test" in item.keywords
        ):
            item.add_marker(skip_marker)
