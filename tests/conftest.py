import os
import sys

import pytest


# Ensure 'src' is on sys.path for package imports in tests
TESTS_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(TESTS_DIR, os.pardir))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


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
