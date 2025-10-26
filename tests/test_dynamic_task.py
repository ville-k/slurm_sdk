"""Tests for dynamic task creation (using task as a function)."""

from slurm import task
from slurm.task import SlurmTask


def test_task_as_function_creates_slurm_task():
    """Test that calling task() as a function creates a SlurmTask."""

    def my_function(x: int, y: int) -> int:
        return x + y

    # Create task dynamically
    my_task = task(my_function, time="00:01:00", mem="2G")

    # Verify it's a SlurmTask
    assert isinstance(my_task, SlurmTask)

    # Verify SBATCH options are set
    assert my_task.sbatch_options["time"] == "00:01:00"
    assert my_task.sbatch_options["mem"] == "2G"
    assert my_task.sbatch_options["job_name"] == "my_function"

    # Verify the unwrapped function works
    assert my_task.unwrapped(5, 3) == 8


def test_dynamic_task_with_container_file():
    """Test dynamic task creation with packaging_dockerfile parameter (new API)."""

    def process_data(data: str) -> str:
        return data.upper()

    # Create task with packaging string and packaging_dockerfile kwarg
    container_task = task(
        process_data,
        time="00:30:00",
        mem="4G",
        packaging="container",
        packaging_dockerfile="path/to/Dockerfile",
    )

    # Verify packaging config
    assert container_task.packaging is not None
    assert container_task.packaging["type"] == "container"
    assert container_task.packaging["dockerfile"] == "path/to/Dockerfile"

    # Verify SBATCH options
    assert container_task.sbatch_options["time"] == "00:30:00"
    assert container_task.sbatch_options["mem"] == "4G"

    # Verify unwrapped function works
    assert container_task.unwrapped("hello") == "HELLO"


def test_multiple_tasks_from_same_function():
    """Test creating multiple task variants from one function."""

    def compute(n: int) -> int:
        return n**2

    # Create different variants
    quick_compute = task(compute, time="00:05:00", mem="1G", job_name="quick")
    heavy_compute = task(compute, time="02:00:00", mem="32G", job_name="heavy")

    # Both should be SlurmTasks
    assert isinstance(quick_compute, SlurmTask)
    assert isinstance(heavy_compute, SlurmTask)

    # Both should have different configurations
    assert quick_compute.sbatch_options["time"] == "00:05:00"
    assert quick_compute.sbatch_options["mem"] == "1G"
    assert quick_compute.sbatch_options["job_name"] == "quick"

    assert heavy_compute.sbatch_options["time"] == "02:00:00"
    assert heavy_compute.sbatch_options["mem"] == "32G"
    assert heavy_compute.sbatch_options["job_name"] == "heavy"

    # Both should execute the same function (using unwrapped for local execution)
    assert quick_compute.unwrapped(5) == 25
    assert heavy_compute.unwrapped(5) == 25


def test_task_function_equals_decorator():
    """Test that task as function and task as decorator are equivalent."""

    # Using decorator
    @task(time="01:00:00", mem="8G", cpus_per_task=4)
    def decorated_func(x: int) -> int:
        return x * 2

    # Using function call
    def plain_func(x: int) -> int:
        return x * 2

    dynamic_func = task(plain_func, time="01:00:00", mem="8G", cpus_per_task=4)

    # Both should be SlurmTasks
    assert isinstance(decorated_func, SlurmTask)
    assert isinstance(dynamic_func, SlurmTask)

    # Both should have same configuration
    assert decorated_func.sbatch_options["time"] == dynamic_func.sbatch_options["time"]
    assert decorated_func.sbatch_options["mem"] == dynamic_func.sbatch_options["mem"]
    assert (
        decorated_func.sbatch_options["cpus_per_task"]
        == dynamic_func.sbatch_options["cpus_per_task"]
    )

    # Both should execute correctly (using unwrapped for local execution)
    assert decorated_func.unwrapped(10) == 20
    assert dynamic_func.unwrapped(10) == 20


def test_dynamic_task_with_custom_job_name():
    """Test that custom job names work with dynamic task creation."""

    def process(data: str) -> str:
        return data

    custom_task = task(process, time="00:10:00", job_name="custom_process_job")

    assert custom_task.sbatch_options["job_name"] == "custom_process_job"


def test_dynamic_task_packaging_with_kwargs():
    """Test that packaging string works correctly with packaging_* kwargs."""

    def my_func(x: int) -> int:
        return x

    # String packaging with packaging_* kwargs
    my_task = task(
        my_func,
        time="00:10:00",
        packaging="container",  # String packaging
        packaging_dockerfile="path/to/Dockerfile",  # Packaging kwargs
        packaging_push=True,
    )

    # Packaging should combine string and kwargs
    assert my_task.packaging["type"] == "container"
    assert my_task.packaging["dockerfile"] == "path/to/Dockerfile"
    assert my_task.packaging["push"] is True
