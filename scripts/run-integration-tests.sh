#!/bin/bash
#
# Run integration tests for the Slurm SDK
#
# This script is designed to be run from the host machine. It:
# 1. Starts the Slurm cluster and registry services if not running
# 2. Runs integration tests inside the dev container
# 3. Optionally keeps services running after tests complete
#
# Usage:
#   ./scripts/run-integration-tests.sh                    # Run all integration tests
#   ./scripts/run-integration-tests.sh -k "test_submit"   # Run specific tests
#   ./scripts/run-integration-tests.sh --keep             # Keep services after tests
#   ./scripts/run-integration-tests.sh -v                 # Verbose output
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/containers/docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
KEEP_SERVICES=false
PYTEST_ARGS=()
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --keep|-k)
            if [[ "${2:-}" == "" ]] || [[ "${2:-}" == -* ]]; then
                KEEP_SERVICES=true
            else
                # -k is pytest's keyword filter
                PYTEST_ARGS+=("$1" "$2")
                shift
            fi
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            PYTEST_ARGS+=("-v")
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS] [PYTEST_ARGS...]"
            echo ""
            echo "Run integration tests for the Slurm SDK."
            echo ""
            echo "Options:"
            echo "  --keep          Keep docker-compose services running after tests"
            echo "  -v, --verbose   Verbose output"
            echo "  -h, --help      Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                           # Run all integration tests"
            echo "  $0 -k 'test_submit'          # Run tests matching 'test_submit'"
            echo "  $0 --keep -v                 # Verbose, keep services"
            echo "  $0 tests/integration/test_container_packaging_basic.py"
            exit 0
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

# Default pytest args if none specified
if [[ ${#PYTEST_ARGS[@]} -eq 0 ]]; then
    PYTEST_ARGS=("--run-integration" "tests/integration/")
elif [[ ! " ${PYTEST_ARGS[*]} " =~ " --run-integration " ]]; then
    # Add --run-integration if not already present
    PYTEST_ARGS=("--run-integration" "${PYTEST_ARGS[@]}")
fi

echo -e "${GREEN}Slurm SDK Integration Test Runner${NC}"
echo "=================================="
echo ""

# Check for docker/podman
if command -v docker &> /dev/null; then
    COMPOSE_CMD="docker compose"
    CONTAINER_CMD="docker"
elif command -v podman &> /dev/null; then
    COMPOSE_CMD="podman compose"
    CONTAINER_CMD="podman"
else
    echo -e "${RED}Error: docker or podman is required${NC}"
    exit 1
fi

# Function to check if services are running
services_running() {
    $COMPOSE_CMD -f "$COMPOSE_FILE" ps --services --filter "status=running" 2>/dev/null | grep -q "slurm"
}

# Function to wait for SSH to be ready
wait_for_ssh() {
    echo -n "Waiting for Slurm SSH..."
    local max_attempts=60
    local attempt=0
    while [[ $attempt -lt $max_attempts ]]; do
        if $CONTAINER_CMD exec slurm-test systemctl is-active sshd &>/dev/null; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        ((attempt++))
    done
    echo -e " ${RED}timeout${NC}"
    return 1
}

# Start services if not running
if services_running; then
    echo -e "${YELLOW}Services already running${NC}"
else
    echo "Starting Slurm cluster and registry..."
    # Create network if it doesn't exist (required since network is external in docker-compose)
    $CONTAINER_CMD network create slurm-dev-network 2>/dev/null || true
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d slurm registry

    # Wait for services to be ready
    sleep 5
    wait_for_ssh || {
        echo -e "${RED}Failed to start Slurm cluster${NC}"
        exit 1
    }

    # Enable oversubscription for workflow tests
    $CONTAINER_CMD exec slurm-test scontrol update PartitionName=debug OverSubscribe=YES 2>/dev/null || true
fi

echo ""
echo "Running integration tests..."
echo "pytest ${PYTEST_ARGS[*]}"
echo ""

# Build dev container if needed
$COMPOSE_CMD -f "$COMPOSE_FILE" build devcontainer 2>/dev/null || true

# Run tests in dev container
set +e
$COMPOSE_CMD -f "$COMPOSE_FILE" run --rm \
    -e SLURM_SDK_DEV_MODE=ci \
    -e SLURM_HOST=slurm \
    -e SLURM_PORT=22 \
    -e REGISTRY_URL=registry:5000 \
    devcontainer \
    bash -c "cd /workspace && uv sync --dev --quiet && uv run pytest ${PYTEST_ARGS[*]}"
TEST_EXIT_CODE=$?
set -e

echo ""

# Cleanup
if [[ "$KEEP_SERVICES" == "true" ]]; then
    echo -e "${YELLOW}Keeping services running (--keep specified)${NC}"
    echo "To stop services: docker compose -f $COMPOSE_FILE down -v"
else
    echo "Stopping services..."
    $COMPOSE_CMD -f "$COMPOSE_FILE" down -v 2>/dev/null || true
fi

# Report result
echo ""
if [[ $TEST_EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}Tests passed!${NC}"
else
    echo -e "${RED}Tests failed with exit code $TEST_EXIT_CODE${NC}"
fi

exit $TEST_EXIT_CODE
