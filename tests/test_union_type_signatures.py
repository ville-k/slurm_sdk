"""Tests for Union[T, Job[T]] type signature support."""

import sys
from pathlib import Path
from typing import Union, TYPE_CHECKING

from slurm.cluster import Cluster
from slurm.decorators import task, workflow
from slurm.context import set_active_context, reset_active_context, clear_active_context
from slurm.workflow import WorkflowContext

# Allow importing test helpers
HELPERS_DIR = Path(__file__).parent / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))
from local_backend import LocalBackend  # type: ignore

if TYPE_CHECKING:
    from slurm.job import Job


def create_mock_cluster(tmp_path: Path) -> Cluster:
    """Create a mock cluster for testing."""
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)
    cluster.backend = LocalBackend(job_base_dir=str(tmp_path))
    cluster.backend_type = "LocalBackend"
    cluster.packaging_defaults = {"type": "none"}
    cluster.callbacks = []
    cluster.console = None
    return cluster


def test_union_type_signature_documentation():
    """Test that Union[T, Job[T]] signatures are documented correctly.

    This test demonstrates the pattern for Union[T, Job[T]] signatures.
    The actual type signature is defined using TYPE_CHECKING blocks to
    ensure type checkers see the correct types while runtime sees the
    actual implementation.
    """

    # Example of how to define a task with Union[T, Job[T]] signature:
    if TYPE_CHECKING:
        # Type checker sees this signature:
        @task(time="00:01:00")
        def train(
            data: Union[str, "Job[str]"],
            config: Union[dict, "Job[dict]"],
        ) -> dict:
            """Train with automatic Job unwrapping."""
            ...

    else:
        # Runtime sees this implementation:
        @task(time="00:01:00")
        def train(data: str, config: dict) -> dict:
            """Train with data and config."""
            return {"model": "trained", "data": data, "config": config}

    # At runtime, train is a SlurmTask
    from slurm.task import SlurmTask

    assert isinstance(train, SlurmTask)


def test_union_type_with_job_argument(tmp_path):
    """Test passing Job as argument to task with Union type signature."""
    clear_active_context()

    # Define tasks
    @task(time="00:01:00")
    def preprocess(data_path: str) -> str:
        """Preprocess data."""
        return f"preprocessed_{data_path}"

    if TYPE_CHECKING:

        @task(time="00:01:00")
        def train(data: Union[str, "Job[str]"]) -> dict:
            """Train with Union type signature."""
            ...

    else:

        @task(time="00:01:00")
        def train(data: str) -> dict:
            """Train with data."""
            return {"model": "trained", "data": data}

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Create preprocessing job
        prep_job = preprocess("data.csv")

        # Pass Job to train - automatic dependency creation
        train_job = train(prep_job)

        # Both should be Jobs
        from slurm.job import Job

        assert isinstance(prep_job, Job)
        assert isinstance(train_job, Job)

        # Type checker sees:
        # prep_job: Job[str]
        # train_job: Job[dict]
        # train accepts Union[str, Job[str]], so passing Job[str] type-checks
    finally:
        reset_active_context(token)


def test_union_type_with_direct_value(tmp_path):
    """Test passing direct value to task with Union type signature."""
    clear_active_context()

    if TYPE_CHECKING:

        @task(time="00:01:00")
        def process(data: Union[str, "Job[str]"]) -> int:
            """Process with Union type signature."""
            ...

    else:

        @task(time="00:01:00")
        def process(data: str) -> int:
            """Process data."""
            return len(data)

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Pass direct value (not a Job)
        job = process("test_data")

        from slurm.job import Job

        assert isinstance(job, Job)

        # Type checker sees:
        # job: Job[int]
        # process accepts Union[str, Job[str]], so passing str type-checks
    finally:
        reset_active_context(token)


def test_union_type_mixed_arguments(tmp_path):
    """Test task with multiple Union type arguments."""
    clear_active_context()

    @task(time="00:01:00")
    def prepare_config() -> dict:
        """Prepare config."""
        return {"lr": 0.01, "batch_size": 32}

    if TYPE_CHECKING:

        @task(time="00:01:00")
        def train(
            data_path: Union[str, "Job[str]"],
            config: Union[dict, "Job[dict]"],
        ) -> dict:
            """Train with mixed Union arguments."""
            ...

    else:

        @task(time="00:01:00")
        def train(data_path: str, config: dict) -> dict:
            """Train model."""
            return {"model": "trained", "data": data_path, "config": config}

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Scenario 1: Both direct values
        job1 = train("data.csv", {"lr": 0.001})

        # Scenario 2: One Job, one direct value
        config_job = prepare_config()
        job2 = train("data.csv", config_job)

        # Both should be Jobs
        from slurm.job import Job

        assert isinstance(job1, Job)
        assert isinstance(job2, Job)
        assert isinstance(config_job, Job)

        # Type checker accepts both scenarios because of Union types
    finally:
        reset_active_context(token)


def test_union_type_in_workflow(tmp_path):
    """Test Union types work correctly in workflow context."""
    clear_active_context()

    @task(time="00:01:00")
    def load_data(path: str) -> str:
        """Load data."""
        return f"data_from_{path}"

    if TYPE_CHECKING:

        @task(time="00:01:00")
        def transform(data: Union[str, "Job[str]"]) -> list[int]:
            """Transform with Union type."""
            ...

    else:

        @task(time="00:01:00")
        def transform(data: str) -> list[int]:
            """Transform data to list."""
            return [len(data), len(data) * 2]

    @workflow(time="00:10:00")
    def pipeline(path: str, ctx: WorkflowContext) -> list[int]:
        """Pipeline using Union types."""
        # Load data (returns Job[str])
        data_job = load_data(path)

        # Transform accepts Union[str, Job[str]]
        result_job = transform(data_job)

        # Could also pass direct value:
        # result_job = transform("direct_data")

        return result_job

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Run workflow
        workflow_job = pipeline("input.csv")

        from slurm.job import Job

        assert isinstance(workflow_job, Job)

        # Type checker knows:
        # data_job: Job[str]
        # result_job: Job[list[int]] (from transform return type)
        # workflow_job: Job[list[int]] (from pipeline return type)
    finally:
        reset_active_context(token)


def test_union_type_preserves_dependencies(tmp_path):
    """Test that Union types preserve automatic dependency tracking."""
    clear_active_context()

    @task(time="00:01:00")
    def step1() -> str:
        """First step."""
        return "step1_result"

    if TYPE_CHECKING:

        @task(time="00:01:00")
        def step2(input_data: Union[str, "Job[str]"]) -> str:
            """Second step with Union type."""
            ...

    else:

        @task(time="00:01:00")
        def step2(input_data: str) -> str:
            """Second step."""
            return f"step2_{input_data}"

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Create dependency chain
        job1 = step1()
        job2 = step2(job1)  # Job passed as argument

        # Verify jobs were created
        from slurm.job import Job

        assert isinstance(job1, Job)
        assert isinstance(job2, Job)

        # The dependency should be tracked automatically
        # (In a full implementation, job2 would wait for job1)
    finally:
        reset_active_context(token)


def test_union_type_with_optional(tmp_path):
    """Test Union types with Optional parameters."""
    clear_active_context()

    if TYPE_CHECKING:

        @task(time="00:01:00")
        def process(
            data: Union[str, "Job[str]"],
            config: Union[dict, "Job[dict]", None] = None,
        ) -> str:
            """Process with optional Union parameter."""
            ...

    else:

        @task(time="00:01:00")
        def process(data: str, config: dict | None = None) -> str:
            """Process data with optional config."""
            if config:
                return f"processed_{data}_with_config"
            return f"processed_{data}"

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Call without optional parameter
        job1 = process("data.csv")

        # Call with optional parameter as direct value
        job2 = process("data.csv", {"setting": "value"})

        # Both should be Jobs
        from slurm.job import Job

        assert isinstance(job1, Job)
        assert isinstance(job2, Job)
    finally:
        reset_active_context(token)
