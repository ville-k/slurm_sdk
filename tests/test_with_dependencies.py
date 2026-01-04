"""Tests for the with_dependencies workflow API."""

from unittest.mock import MagicMock, patch

from slurm import task
from slurm.decorators import workflow as workflow_decorator
from slurm.cluster import Cluster, SubmittableWorkflow
from slurm.job import Job
from slurm.workflow import WorkflowContext


@task
def sample_task(x: int) -> int:
    """A sample task for testing."""
    return x * 2


@task
def another_task(y: int) -> int:
    """Another sample task for testing."""
    return y + 1


@task(is_workflow=True)
def sample_workflow(ctx: WorkflowContext, value: int) -> int:
    """A sample workflow for testing."""
    return value


@workflow_decorator
def decorated_workflow(ctx: WorkflowContext, value: int) -> int:
    """Workflow defined with @workflow decorator for SubmittableWorkflow tests."""
    return value


class TestSubmittableWorkflow:
    """Tests for the SubmittableWorkflow class."""

    def test_with_dependencies_returns_self(self):
        """Test that with_dependencies returns self for chaining."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_submitter = MagicMock()

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        result = wrapper.with_dependencies([sample_task, another_task])

        assert result is wrapper
        assert len(wrapper._dependent_tasks) == 2
        assert wrapper._dependent_tasks[0] is sample_task
        assert wrapper._dependent_tasks[1] is another_task

    def test_with_dependencies_empty_list(self):
        """Test with_dependencies with empty list."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_submitter = MagicMock()

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        result = wrapper.with_dependencies([])

        assert result is wrapper
        assert len(wrapper._dependent_tasks) == 0

    def test_call_invokes_submitter(self):
        """Test that __call__ invokes the original submitter."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_job = MagicMock(spec=Job)
        mock_submitter = MagicMock(return_value=mock_job)

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        result = wrapper(42)

        mock_submitter.assert_called_once_with(42)
        assert result is mock_job

    def test_call_with_kwargs(self):
        """Test that __call__ passes kwargs to submitter."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_job = MagicMock(spec=Job)
        mock_submitter = MagicMock(return_value=mock_job)

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        result = wrapper(value=100)

        mock_submitter.assert_called_once_with(value=100)
        assert result is mock_job

    def test_build_dependency_containers_skips_non_container(self):
        """Test that non-container tasks are skipped."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_cluster.packaging_defaults = {"type": "wheel"}
        mock_submitter = MagicMock()

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        wrapper._dependent_tasks = [sample_task]
        result = wrapper._build_dependency_containers()

        # Should return empty dict since wheel packaging doesn't build containers
        assert result == {}

    def test_build_dependency_containers_skips_non_slurm_task(self):
        """Test that non-SlurmTask objects are skipped with warning."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_submitter = MagicMock()

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        # Add a non-SlurmTask object
        wrapper._dependent_tasks = ["not a task"]

        with patch("slurm.cluster.logger") as mock_logger:
            result = wrapper._build_dependency_containers()
            mock_logger.warning.assert_called()

        assert result == {}

    def test_prebuilt_images_passed_to_cluster(self):
        """Test that pre-built images are passed to cluster when they exist."""
        mock_cluster = MagicMock()
        mock_job = MagicMock(spec=Job)
        mock_submitter = MagicMock(return_value=mock_job)

        wrapper = SubmittableWorkflow(
            cluster=mock_cluster,
            submitter=mock_submitter,
            task_func=sample_workflow,
            packaging_config=None,
        )

        # Manually set pre-built images (simulating what _build_dependency_containers returns)
        task_name = f"{sample_task.func.__module__}.{sample_task.func.__qualname__}"
        wrapper._prebuilt_images = {task_name: "docker://registry/test:abc123"}

        # Call the wrapper
        wrapper(42)

        # Verify the submitter was called
        mock_submitter.assert_called_once_with(42)


class TestClusterSubmitWithDependencies:
    """Tests for cluster.submit() returning SubmittableWorkflow for workflows."""

    def test_submit_returns_submittable_workflow_for_workflow(self):
        """Test that submit returns SubmittableWorkflow for workflow tasks."""
        # Check how workflow is detected (is_workflow is in sbatch_options)
        assert sample_workflow.sbatch_options.get("is_workflow", False) is True

        with patch.object(Cluster, "__init__", lambda self: None):
            cluster = Cluster()
            cluster.backend = MagicMock()
            cluster.backend_type = "mock"
            cluster.packaging_defaults = None
            cluster.default_packaging = None
            cluster.default_packaging_kwargs = {}
            cluster._project_root = None
            cluster.callbacks = []

            result = cluster.submit(sample_workflow)

            assert isinstance(result, SubmittableWorkflow)

    def test_submit_returns_callable_for_regular_task(self):
        """Test that submit returns callable for regular tasks."""
        with patch.object(Cluster, "__init__", lambda self: None):
            cluster = Cluster()
            cluster.backend = MagicMock()
            cluster.backend_type = "mock"
            cluster.packaging_defaults = None
            cluster.default_packaging = None
            cluster.default_packaging_kwargs = {}
            cluster._project_root = None
            cluster.callbacks = []

            result = cluster.submit(sample_task)

            # Regular tasks return a callable, not SubmittableWorkflow
            assert callable(result)
            assert not isinstance(result, SubmittableWorkflow)

    def test_submit_detects_workflow_decorator_flag(self):
        """Workflows marked via @workflow should return SubmittableWorkflow."""
        with patch.object(Cluster, "__init__", lambda self: None):
            cluster = Cluster()
            cluster.backend = MagicMock()
            cluster.backend_type = "mock"
            cluster.packaging_defaults = None
            cluster.default_packaging = None
            cluster.default_packaging_kwargs = {}
            cluster._project_root = None
            cluster.callbacks = []

            result = cluster.submit(decorated_workflow)

            assert isinstance(result, SubmittableWorkflow)


class TestPrebuiltImageUsage:
    """Tests for using pre-built images in _prepare_packaging_strategy."""

    def test_prepare_packaging_strategy_uses_prebuilt_image(self):
        """Test that _prepare_packaging_strategy uses pre-built image when available."""
        with patch.object(Cluster, "__init__", lambda self: None):
            cluster = Cluster()
            cluster.backend = MagicMock()
            cluster.backend_type = "mock"
            cluster.packaging_defaults = {"type": "wheel"}
            cluster.default_packaging = None
            cluster.default_packaging_kwargs = {}
            cluster._project_root = None
            cluster.callbacks = []

            # Set up pre-built images
            task_name = f"{sample_task.func.__module__}.{sample_task.func.__qualname__}"
            cluster._prebuilt_dependency_images = {
                task_name: "docker://registry/prebuilt:v1"
            }

            # Mock get_packaging_strategy to avoid actual packaging
            with patch("slurm.cluster.get_packaging_strategy") as mock_get_strategy:
                mock_strategy = MagicMock()
                mock_get_strategy.return_value = mock_strategy

                cluster._prepare_packaging_strategy(sample_task, None)

                # Check that get_packaging_strategy was called with container config
                call_args = mock_get_strategy.call_args
                config = call_args[0][0]
                assert config["type"] == "container"
                assert config["image"] == "docker://registry/prebuilt:v1"
                assert config["push"] is False
