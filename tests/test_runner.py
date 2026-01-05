"""Unit tests for runner module helper functions.

Tests for:
- _run_callbacks()
- _function_wants_workflow_context()
- _bind_workflow_context()
- _write_environment_metadata()
- runner.callbacks.run_callbacks() (new module)
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from slurm.callbacks.callbacks import BaseCallback
from slurm.runner import (
    _bind_workflow_context,
    _function_wants_workflow_context,
    _run_callbacks,
    _write_environment_metadata,
)
from slurm.runner.callbacks import run_callbacks
from slurm.runner.context_manager import (
    bind_workflow_context,
    function_wants_workflow_context,
)
from slurm.runner.argument_loader import (
    RunnerArgs,
    create_argument_parser,
    load_callbacks,
    load_task_arguments,
    log_startup_info,
    parse_args,
    restore_sys_path,
)
from slurm.runner.workflow_builder import (
    ClusterConfig,
    WorkflowSetupResult,
    cleanup_cluster_connections,
    create_workflow_context,
    deactivate_workflow_context,
    determine_parent_packaging_type,
    extract_cluster_config_from_env,
    teardown_workflow_execution,
)
from slurm.runner.placeholder import (
    resolve_placeholder,
    resolve_task_arguments,
)
from slurm.runner.result_saver import (
    save_result,
    update_job_metadata,
    write_environment_metadata,
)
from slurm.workflow import WorkflowContext


class TestRunCallbacks:
    """Tests for _run_callbacks helper function."""

    def test_run_callbacks_calls_method_on_all_callbacks(self):
        """Test that method is called on all callbacks."""
        callback1 = MagicMock(spec=BaseCallback)
        callback1.should_run_on_runner.return_value = True
        callback2 = MagicMock(spec=BaseCallback)
        callback2.should_run_on_runner.return_value = True

        _run_callbacks(
            [callback1, callback2], "on_begin_run_job_ctx", "arg1", kwarg="val"
        )

        callback1.on_begin_run_job_ctx.assert_called_once_with("arg1", kwarg="val")
        callback2.on_begin_run_job_ctx.assert_called_once_with("arg1", kwarg="val")

    def test_run_callbacks_skips_non_runner_methods(self):
        """Test that callbacks with should_run_on_runner=False are skipped."""

        class TestCallback(BaseCallback):
            def __init__(self):
                super().__init__()
                self.method_called = False

            def should_run_on_runner(self, method_name: str) -> bool:
                return False

            def on_submitted_ctx(self, *args, **kwargs):
                self.method_called = True

        callback = TestCallback()
        _run_callbacks([callback], "on_submitted_ctx", "arg")

        assert callback.method_called is False

    def test_run_callbacks_catches_exceptions(self):
        """Test that exceptions in callbacks are caught and logged."""

        class ErrorCallback(BaseCallback):
            def on_begin_run_job_ctx(self, *args, **kwargs):
                raise RuntimeError("Callback error")

        class TrackingCallback(BaseCallback):
            def __init__(self):
                super().__init__()
                self.called_with = None

            def on_begin_run_job_ctx(self, *args, **kwargs):
                self.called_with = (args, kwargs)

        callback1 = ErrorCallback()
        callback2 = TrackingCallback()

        # Should not raise, and callback2 should still be called
        _run_callbacks([callback1, callback2], "on_begin_run_job_ctx", "arg")

        assert callback2.called_with == (("arg",), {})

    def test_run_callbacks_empty_list(self):
        """Test that empty callback list works."""
        _run_callbacks([], "on_begin_run_job_ctx", "arg")
        # Should not raise

    def test_run_callbacks_without_should_run_on_runner(self):
        """Test callbacks without should_run_on_runner are still called."""
        callback = MagicMock()
        # No should_run_on_runner attribute
        del callback.should_run_on_runner

        _run_callbacks([callback], "test_method", "arg")

        callback.test_method.assert_called_once_with("arg")


class TestRunCallbacksModule:
    """Tests for the new runner.callbacks module."""

    def test_run_callbacks_from_module(self):
        """Test that run_callbacks from the module works the same as _run_callbacks."""

        class TrackingCallback(BaseCallback):
            def __init__(self):
                super().__init__()
                self.called_with = None

            def on_begin_run_job_ctx(self, *args, **kwargs):
                self.called_with = (args, kwargs)

        callback = TrackingCallback()
        run_callbacks([callback], "on_begin_run_job_ctx", "arg1", key="val")

        assert callback.called_with == (("arg1",), {"key": "val"})

    def test_run_callbacks_module_catches_exceptions(self):
        """Test that module function catches exceptions."""

        class ErrorCallback(BaseCallback):
            def on_begin_run_job_ctx(self, *args, **kwargs):
                raise RuntimeError("Error!")

        # Should not raise
        run_callbacks([ErrorCallback()], "on_begin_run_job_ctx", "arg")


class TestContextManagerModule:
    """Tests for the new runner.context_manager module."""

    def test_function_wants_workflow_context_from_module(self):
        """Test that function_wants_workflow_context from module works."""

        def func_with_ctx(x: int, ctx: WorkflowContext) -> int:
            return x

        def func_without_ctx(x: int, y: str) -> int:
            return x

        assert function_wants_workflow_context(func_with_ctx) is True
        assert function_wants_workflow_context(func_without_ctx) is False

    def test_bind_workflow_context_from_module(self, tmp_path):
        """Test that bind_workflow_context from module works."""
        from slurm.cluster import Cluster

        cluster = object.__new__(Cluster)
        workflow_ctx = WorkflowContext(
            cluster=cluster,
            workflow_job_id="test_123",
            workflow_job_dir=tmp_path / "workflow",
            shared_dir=tmp_path / "workflow" / "shared",
            local_mode=True,
        )

        def func(x: int, ctx: WorkflowContext) -> int:
            return x

        args, kwargs, injected = bind_workflow_context(func, (10,), {}, workflow_ctx)

        assert injected is True
        assert kwargs["ctx"] is workflow_ctx


class TestResultSaverModule:
    """Tests for the new runner.result_saver module."""

    def test_write_environment_metadata_from_module(self, tmp_path):
        """Test write_environment_metadata from module."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="wheel",
            job_id="12345",
            workflow_name="test_workflow",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        assert metadata_path.exists()

        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["packaging_type"] == "wheel"
        assert metadata["parent_job"]["slurm_job_id"] == "12345"

    def test_save_result(self, tmp_path):
        """Test save_result function."""
        import pickle

        output_file = tmp_path / "result.pkl"
        result = {"key": "value", "number": 42}

        save_result(str(output_file), result)

        assert output_file.exists()
        with open(output_file, "rb") as f:
            loaded = pickle.load(f)
        assert loaded == result

    def test_save_result_creates_directory(self, tmp_path):
        """Test save_result creates output directory if needed."""
        import pickle

        output_file = tmp_path / "subdir" / "result.pkl"
        result = [1, 2, 3]

        save_result(str(output_file), result)

        assert output_file.exists()
        with open(output_file, "rb") as f:
            loaded = pickle.load(f)
        assert loaded == result

    def test_update_job_metadata(self, tmp_path):
        """Test update_job_metadata function."""
        output_file = tmp_path / "result.pkl"
        output_file.write_bytes(b"dummy")

        update_job_metadata(
            output_file=str(output_file),
            job_id="job_123",
            timestamp=1234567890.0,
        )

        metadata_path = tmp_path / "metadata.json"
        assert metadata_path.exists()

        with open(metadata_path) as f:
            metadata = json.load(f)

        assert "job_123" in metadata
        assert metadata["job_123"]["result_file"] == "result.pkl"
        assert metadata["job_123"]["timestamp"] == 1234567890.0

    def test_update_job_metadata_merges(self, tmp_path):
        """Test update_job_metadata merges with existing metadata."""
        # Create initial metadata
        metadata_path = tmp_path / "metadata.json"
        initial = {"existing_job": {"result_file": "old.pkl", "timestamp": 100.0}}
        with open(metadata_path, "w") as f:
            json.dump(initial, f)

        output_file = tmp_path / "new_result.pkl"
        output_file.write_bytes(b"dummy")

        update_job_metadata(
            output_file=str(output_file),
            job_id="new_job",
            timestamp=200.0,
        )

        with open(metadata_path) as f:
            metadata = json.load(f)

        # Both entries should exist
        assert "existing_job" in metadata
        assert "new_job" in metadata
        assert metadata["new_job"]["result_file"] == "new_result.pkl"


class TestFunctionWantsWorkflowContext:
    """Tests for _function_wants_workflow_context helper function."""

    def test_detects_workflow_context_by_annotation(self):
        """Test detection by WorkflowContext type annotation."""

        def func(x: int, ctx: WorkflowContext) -> int:
            return x

        assert _function_wants_workflow_context(func) is True

    def test_detects_workflow_context_by_string_annotation(self):
        """Test detection by string 'WorkflowContext' annotation."""

        def func(x: int, ctx: "WorkflowContext") -> int:
            return x

        assert _function_wants_workflow_context(func) is True

    def test_detects_workflow_context_by_ctx_parameter_name(self):
        """Test detection by 'ctx' parameter name."""

        def func(x: int, ctx) -> int:
            return x

        assert _function_wants_workflow_context(func) is True

    def test_detects_workflow_context_by_context_parameter_name(self):
        """Test detection by 'context' parameter name."""

        def func(x: int, context) -> int:
            return x

        assert _function_wants_workflow_context(func) is True

    def test_detects_workflow_context_by_workflow_context_parameter_name(self):
        """Test detection by 'workflow_context' parameter name."""

        def func(x: int, workflow_context) -> int:
            return x

        assert _function_wants_workflow_context(func) is True

    def test_returns_false_for_no_context_parameter(self):
        """Test returns False when no context parameter."""

        def func(x: int, y: str) -> int:
            return x

        assert _function_wants_workflow_context(func) is False

    def test_returns_false_for_job_context_annotation(self):
        """Test returns False for JobContext annotation (not WorkflowContext)."""
        from slurm.runtime import JobContext

        # Use a parameter name that doesn't trigger the name-based detection
        def func(x: int, job_ctx: JobContext) -> int:
            return x

        assert _function_wants_workflow_context(func) is False

    def test_handles_inspection_errors(self):
        """Test gracefully handles functions that can't be inspected."""
        # Built-in functions can't always be inspected
        assert _function_wants_workflow_context(len) is False


class TestBindWorkflowContext:
    """Tests for _bind_workflow_context helper function."""

    def create_workflow_context(self, tmp_path: Path) -> WorkflowContext:
        """Create a mock WorkflowContext for testing."""
        from slurm.cluster import Cluster

        cluster = object.__new__(Cluster)
        return WorkflowContext(
            cluster=cluster,
            workflow_job_id="test_123",
            workflow_job_dir=tmp_path / "workflow",
            shared_dir=tmp_path / "workflow" / "shared",
            local_mode=True,
        )

    def test_injects_context_by_annotation(self, tmp_path):
        """Test context injection by type annotation."""
        workflow_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, ctx: WorkflowContext) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(func, (10,), {}, workflow_ctx)

        assert injected is True
        assert args == (10,)
        assert "ctx" in kwargs
        assert kwargs["ctx"] is workflow_ctx

    def test_injects_context_by_name_ctx(self, tmp_path):
        """Test context injection by 'ctx' parameter name."""
        workflow_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, ctx) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(func, (10,), {}, workflow_ctx)

        assert injected is True
        assert "ctx" in kwargs

    def test_injects_context_by_name_context(self, tmp_path):
        """Test context injection by 'context' parameter name."""
        workflow_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, context) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(func, (10,), {}, workflow_ctx)

        assert injected is True
        assert "context" in kwargs

    def test_injects_context_by_name_workflow_context(self, tmp_path):
        """Test context injection by 'workflow_context' parameter name."""
        workflow_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, workflow_context) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(func, (10,), {}, workflow_ctx)

        assert injected is True
        assert "workflow_context" in kwargs

    def test_does_not_inject_if_already_provided(self, tmp_path):
        """Test that context is not injected if already in kwargs."""
        workflow_ctx = self.create_workflow_context(tmp_path)
        existing_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, ctx: WorkflowContext) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(
            func, (10,), {"ctx": existing_ctx}, workflow_ctx
        )

        assert injected is False
        assert kwargs["ctx"] is existing_ctx  # Original preserved

    def test_returns_false_if_no_context_param(self, tmp_path):
        """Test returns False when function doesn't want context."""
        workflow_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, y: str) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(
            func, (10,), {"y": "hello"}, workflow_ctx
        )

        assert injected is False
        assert kwargs == {"y": "hello"}

    def test_preserves_existing_kwargs(self, tmp_path):
        """Test that existing kwargs are preserved."""
        workflow_ctx = self.create_workflow_context(tmp_path)

        def func(x: int, y: str, ctx: WorkflowContext) -> int:
            return x

        args, kwargs, injected = _bind_workflow_context(
            func, (10,), {"y": "hello"}, workflow_ctx
        )

        assert injected is True
        assert kwargs["y"] == "hello"
        assert kwargs["ctx"] is workflow_ctx


class TestWriteEnvironmentMetadata:
    """Tests for _write_environment_metadata helper function."""

    def test_writes_metadata_file(self, tmp_path):
        """Test that metadata file is created."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="wheel",
            job_id="12345",
            workflow_name="test_workflow",
            pre_submission_id="pre_123",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        assert metadata_path.exists()

    def test_metadata_contains_required_fields(self, tmp_path):
        """Test that metadata contains all required fields."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="container",
            job_id="12345",
            workflow_name="test_workflow",
            pre_submission_id="pre_123",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["version"] == "1.0"
        assert metadata["packaging_type"] == "container"
        assert "environment" in metadata
        assert "shared_paths" in metadata
        assert "parent_job" in metadata
        assert "created_at" in metadata

    def test_metadata_environment_section(self, tmp_path):
        """Test environment section in metadata."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="wheel",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        env = metadata["environment"]
        assert "venv_path" in env
        assert "python_executable" in env
        assert "python_version" in env
        assert "container_image" in env
        assert "activated" in env

    def test_metadata_shared_paths_section(self, tmp_path):
        """Test shared_paths section in metadata."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="wheel",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        paths = metadata["shared_paths"]
        assert paths["job_dir"] == str(job_dir)
        assert paths["shared_dir"] == str(job_dir / "shared")
        assert paths["tasks_dir"] == str(job_dir / "tasks")

    def test_metadata_parent_job_section(self, tmp_path):
        """Test parent_job section in metadata."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="wheel",
            job_id="12345",
            workflow_name="my_workflow",
            pre_submission_id="pre_456",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        parent = metadata["parent_job"]
        assert parent["slurm_job_id"] == "12345"
        assert parent["workflow_name"] == "my_workflow"
        assert parent["pre_submission_id"] == "pre_456"

    def test_metadata_defaults_when_optional_params_missing(self, tmp_path):
        """Test default values when optional params not provided."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="none",
        )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        parent = metadata["parent_job"]
        assert parent["slurm_job_id"] == "unknown"
        assert parent["workflow_name"] == "unknown"
        assert parent["pre_submission_id"] == "unknown"

    def test_handles_nonexistent_directory(self, tmp_path):
        """Test gracefully handles when directory doesn't exist."""
        job_dir = tmp_path / "nonexistent"

        # Should not raise - just logs warning
        _write_environment_metadata(
            job_dir=str(job_dir),
            packaging_type="wheel",
        )

    def test_detects_virtual_env(self, tmp_path):
        """Test detection of virtual environment."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        with patch.dict(os.environ, {"VIRTUAL_ENV": "/path/to/venv"}):
            _write_environment_metadata(
                job_dir=str(job_dir),
                packaging_type="wheel",
            )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["environment"]["venv_path"] == "/path/to/venv"
        assert metadata["environment"]["activated"] is True

    def test_detects_container_image(self, tmp_path):
        """Test detection of container image from environment."""
        job_dir = tmp_path / "job"
        job_dir.mkdir()

        # Clear VIRTUAL_ENV to test container detection
        env = {"SINGULARITY_NAME": "my-container.sif"}
        with patch.dict(os.environ, env, clear=False):
            # Also need to clear VIRTUAL_ENV if set
            os.environ.pop("VIRTUAL_ENV", None)
            _write_environment_metadata(
                job_dir=str(job_dir),
                packaging_type="container",
            )

        metadata_path = job_dir / ".slurm_environment.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        assert metadata["environment"]["container_image"] == "my-container.sif"


class TestPlaceholderModule:
    """Tests for runner.placeholder module."""

    def test_resolve_placeholder_returns_primitive_unchanged(self):
        """Test that primitives are returned unchanged."""
        assert resolve_placeholder(42) == 42
        assert resolve_placeholder("hello") == "hello"
        assert resolve_placeholder(3.14) == 3.14
        assert resolve_placeholder(None) is None
        assert resolve_placeholder(True) is True

    def test_resolve_placeholder_preserves_list(self):
        """Test that lists without placeholders are preserved."""
        result = resolve_placeholder([1, 2, 3])
        assert result == [1, 2, 3]
        assert isinstance(result, list)

    def test_resolve_placeholder_preserves_tuple(self):
        """Test that tuples without placeholders are preserved."""
        result = resolve_placeholder((1, 2, 3))
        assert result == (1, 2, 3)
        assert isinstance(result, tuple)

    def test_resolve_placeholder_preserves_dict(self):
        """Test that dicts without placeholders are preserved."""
        result = resolve_placeholder({"a": 1, "b": 2})
        assert result == {"a": 1, "b": 2}

    def test_resolve_placeholder_handles_nested_structures(self):
        """Test resolving nested structures without placeholders."""
        nested = {
            "list": [1, 2, {"nested": (3, 4)}],
            "tuple": (5, 6),
            "value": "test",
        }
        result = resolve_placeholder(nested)
        assert result == nested

    def test_resolve_placeholder_resolves_job_result(self, tmp_path):
        """Test resolving a JobResultPlaceholder."""
        import pickle

        from slurm.task import JobResultPlaceholder

        # Set up job directory with metadata and result file
        job_dir = tmp_path / "job_dir"
        job_dir.mkdir()

        # Create metadata.json
        metadata = {
            "12345": {
                "result_file": "result_12345.pkl",
                "timestamp": 1234567890.0,
            }
        }
        with open(job_dir / "metadata.json", "w") as f:
            json.dump(metadata, f)

        # Create result file
        expected_result = {"status": "success", "value": 42}
        with open(job_dir / "result_12345.pkl", "wb") as f:
            pickle.dump(expected_result, f)

        # Create placeholder
        placeholder = JobResultPlaceholder(job_id="12345")

        # Resolve it
        result = resolve_placeholder(placeholder, job_base_dir=str(tmp_path))

        assert result == expected_result

    def test_resolve_placeholder_resolves_nested_placeholder(self, tmp_path):
        """Test resolving a placeholder nested in a structure."""
        import pickle

        from slurm.task import JobResultPlaceholder

        # Set up job directory
        job_dir = tmp_path / "job_dir"
        job_dir.mkdir()

        metadata = {"job_A": {"result_file": "result_A.pkl", "timestamp": 1.0}}
        with open(job_dir / "metadata.json", "w") as f:
            json.dump(metadata, f)

        with open(job_dir / "result_A.pkl", "wb") as f:
            pickle.dump(100, f)

        # Nested structure with placeholder
        nested = {
            "x": 1,
            "dep": JobResultPlaceholder(job_id="job_A"),
            "list": [2, 3],
        }

        result = resolve_placeholder(nested, job_base_dir=str(tmp_path))

        assert result["x"] == 1
        assert result["dep"] == 100
        assert result["list"] == [2, 3]

    def test_resolve_placeholder_raises_on_missing_job(self, tmp_path):
        """Test that missing job result raises FileNotFoundError."""
        from slurm.task import JobResultPlaceholder

        placeholder = JobResultPlaceholder(job_id="nonexistent")

        with pytest.raises(FileNotFoundError, match="Could not find result file"):
            resolve_placeholder(placeholder, job_base_dir=str(tmp_path))

    def test_resolve_task_arguments(self, tmp_path):
        """Test resolving both args and kwargs."""
        import pickle

        from slurm.task import JobResultPlaceholder

        # Set up job directory
        job_dir = tmp_path / "job_dir"
        job_dir.mkdir()

        metadata = {"job_X": {"result_file": "result_X.pkl", "timestamp": 1.0}}
        with open(job_dir / "metadata.json", "w") as f:
            json.dump(metadata, f)

        with open(job_dir / "result_X.pkl", "wb") as f:
            pickle.dump("resolved_value", f)

        # Create args and kwargs with placeholder
        args = (1, JobResultPlaceholder(job_id="job_X"))
        kwargs = {"key": "value"}

        resolved_args, resolved_kwargs = resolve_task_arguments(
            args, kwargs, job_base_dir=str(tmp_path)
        )

        assert resolved_args == (1, "resolved_value")
        assert resolved_kwargs == {"key": "value"}

    def test_resolve_placeholder_list_in_list(self, tmp_path):
        """Test that nested lists are resolved properly."""
        result = resolve_placeholder([[1, 2], [3, 4]])
        assert result == [[1, 2], [3, 4]]
        assert isinstance(result, list)
        assert isinstance(result[0], list)

    def test_resolve_placeholder_empty_structures(self):
        """Test that empty structures are handled."""
        assert resolve_placeholder([]) == []
        assert resolve_placeholder({}) == {}
        assert resolve_placeholder(()) == ()


class TestArgumentLoaderModule:
    """Tests for runner.argument_loader module."""

    def test_parse_args_regular_job(self):
        """Test parsing arguments for a regular job."""
        argv = [
            "--module",
            "my.module",
            "--function",
            "my_function",
            "--args-file",
            "/path/to/args.pkl",
            "--kwargs-file",
            "/path/to/kwargs.pkl",
            "--output-file",
            "/path/to/output.pkl",
            "--callbacks-file",
            "/path/to/callbacks.pkl",
        ]

        args = parse_args(argv)

        assert args.module == "my.module"
        assert args.function == "my_function"
        assert args.args_file == "/path/to/args.pkl"
        assert args.kwargs_file == "/path/to/kwargs.pkl"
        assert args.output_file == "/path/to/output.pkl"
        assert args.callbacks_file == "/path/to/callbacks.pkl"
        assert args.is_array_job is False

    def test_parse_args_array_job(self):
        """Test parsing arguments for an array job."""
        argv = [
            "--module",
            "my.module",
            "--function",
            "my_function",
            "--array-index",
            "5",
            "--array-items-file",
            "/path/to/items.pkl",
            "--output-file",
            "/path/to/output.pkl",
            "--callbacks-file",
            "/path/to/callbacks.pkl",
        ]

        args = parse_args(argv)

        assert args.module == "my.module"
        assert args.function == "my_function"
        assert args.array_index == 5
        assert args.array_items_file == "/path/to/items.pkl"
        assert args.is_array_job is True

    def test_parse_args_optional_arguments(self):
        """Test parsing optional arguments."""
        argv = [
            "--module",
            "my.module",
            "--function",
            "my_function",
            "--output-file",
            "/path/to/output.pkl",
            "--callbacks-file",
            "/path/to/callbacks.pkl",
            "--loglevel",
            "DEBUG",
            "--job-dir",
            "/path/to/job",
            "--stdout-path",
            "/path/to/stdout",
            "--stderr-path",
            "/path/to/stderr",
            "--pre-submission-id",
            "abc123",
            "--sys-path",
            "encoded_path",
        ]

        args = parse_args(argv)

        assert args.loglevel == "DEBUG"
        assert args.job_dir == "/path/to/job"
        assert args.stdout_path == "/path/to/stdout"
        assert args.stderr_path == "/path/to/stderr"
        assert args.pre_submission_id == "abc123"
        assert args.sys_path == "encoded_path"

    def test_parse_args_default_loglevel(self):
        """Test that default loglevel is INFO."""
        argv = [
            "--module",
            "my.module",
            "--function",
            "my_function",
            "--output-file",
            "/path/to/output.pkl",
            "--callbacks-file",
            "/path/to/callbacks.pkl",
        ]

        args = parse_args(argv)

        assert args.loglevel == "INFO"

    def test_runner_args_is_array_job_property(self):
        """Test the is_array_job property."""
        # Not an array job
        regular_args = RunnerArgs(
            module="m",
            function="f",
            output_file="o",
            callbacks_file="c",
        )
        assert regular_args.is_array_job is False

        # Is an array job
        array_args = RunnerArgs(
            module="m",
            function="f",
            output_file="o",
            callbacks_file="c",
            array_index=0,
        )
        assert array_args.is_array_job is True

    def test_create_argument_parser(self):
        """Test that the argument parser is created correctly."""
        parser = create_argument_parser()

        assert parser is not None
        # Test that it parses minimal required args without error
        args = parser.parse_args(
            [
                "--module",
                "m",
                "--function",
                "f",
                "--output-file",
                "o",
                "--callbacks-file",
                "c",
            ]
        )
        assert args.module == "m"

    def test_log_startup_info_regular_job(self, caplog):
        """Test that startup info is logged for regular jobs."""
        import logging

        caplog.set_level(logging.DEBUG)

        args = RunnerArgs(
            module="my.module",
            function="my_function",
            output_file="/path/to/output.pkl",
            callbacks_file="/path/to/callbacks.pkl",
            args_file="/path/to/args.pkl",
            kwargs_file="/path/to/kwargs.pkl",
            job_dir="/path/to/job",
        )

        log_startup_info(args)

        assert "RUNNER STARTING" in caplog.text
        assert "my.module" in caplog.text
        assert "my_function" in caplog.text
        assert "Starting task execution" in caplog.text

    def test_log_startup_info_array_job(self, caplog):
        """Test that startup info is logged for array jobs."""
        import logging

        caplog.set_level(logging.INFO)

        args = RunnerArgs(
            module="my.module",
            function="my_function",
            output_file="/path/to/output.pkl",
            callbacks_file="/path/to/callbacks.pkl",
            array_index=3,
            array_items_file="/path/to/items.pkl",
            job_dir="/path/to/job",
        )

        log_startup_info(args)

        assert "RUNNER STARTING" in caplog.text
        assert "Starting array task execution" in caplog.text
        assert "index=3" in caplog.text

    def test_restore_sys_path(self):
        """Test restoring sys.path from encoded string."""
        import base64
        import pickle

        # Save original sys.path
        original_path = sys.path.copy()

        try:
            # Create encoded sys.path
            test_paths = ["/custom/path1", "/custom/path2"]
            encoded = base64.b64encode(pickle.dumps(test_paths)).decode()

            restore_sys_path(encoded)

            # Verify test paths are at the beginning
            assert sys.path[0] == "/custom/path1"
            assert sys.path[1] == "/custom/path2"
        finally:
            # Restore original sys.path
            sys.path = original_path

    def test_load_task_arguments_regular_job(self, tmp_path):
        """Test loading arguments for a regular job."""
        import pickle

        # Create args and kwargs files
        args_file = tmp_path / "args.pkl"
        kwargs_file = tmp_path / "kwargs.pkl"

        with open(args_file, "wb") as f:
            pickle.dump((1, 2, 3), f)
        with open(kwargs_file, "wb") as f:
            pickle.dump({"x": 10, "y": 20}, f)

        runner_args = RunnerArgs(
            module="m",
            function="f",
            output_file="o",
            callbacks_file="c",
            args_file=str(args_file),
            kwargs_file=str(kwargs_file),
        )

        task_args, task_kwargs = load_task_arguments(runner_args)

        assert task_args == (1, 2, 3)
        assert task_kwargs == {"x": 10, "y": 20}

    def test_load_task_arguments_array_job_with_dict(self, tmp_path):
        """Test loading array job with dict items."""
        from slurm.array_items import serialize_array_items

        # Create array items file with dict items
        items = [{"a": 1}, {"a": 2}, {"a": 3}]
        serialize_array_items(items, str(tmp_path))
        items_file = tmp_path / "array_items.pkl"

        runner_args = RunnerArgs(
            module="m",
            function="f",
            output_file="o",
            callbacks_file="c",
            array_index=1,
            array_items_file=str(items_file),
        )

        task_args, task_kwargs = load_task_arguments(runner_args)

        assert task_args == ()
        assert task_kwargs == {"a": 2}

    def test_load_task_arguments_array_job_with_tuple(self, tmp_path):
        """Test loading array job with tuple items."""
        from slurm.array_items import serialize_array_items

        items = [(1, 2), (3, 4), (5, 6)]
        serialize_array_items(items, str(tmp_path))
        items_file = tmp_path / "array_items.pkl"

        runner_args = RunnerArgs(
            module="m",
            function="f",
            output_file="o",
            callbacks_file="c",
            array_index=2,
            array_items_file=str(items_file),
        )

        task_args, task_kwargs = load_task_arguments(runner_args)

        assert task_args == (5, 6)
        assert task_kwargs == {}

    def test_load_task_arguments_array_job_with_single_value(self, tmp_path):
        """Test loading array job with single value items."""
        from slurm.array_items import serialize_array_items

        items = [10, 20, 30]
        serialize_array_items(items, str(tmp_path))
        items_file = tmp_path / "array_items.pkl"

        runner_args = RunnerArgs(
            module="m",
            function="f",
            output_file="o",
            callbacks_file="c",
            array_index=0,
            array_items_file=str(items_file),
        )

        task_args, task_kwargs = load_task_arguments(runner_args)

        assert task_args == (10,)
        assert task_kwargs == {}

    def test_load_callbacks(self, tmp_path):
        """Test loading callbacks from file."""
        import pickle

        callbacks_file = tmp_path / "callbacks.pkl"
        # Use BaseCallback directly since local classes can't be pickled
        callbacks = [BaseCallback(), BaseCallback()]

        with open(callbacks_file, "wb") as f:
            pickle.dump(callbacks, f)

        loaded = load_callbacks(str(callbacks_file))

        assert len(loaded) == 2
        assert all(isinstance(cb, BaseCallback) for cb in loaded)

    def test_load_callbacks_empty_file(self, tmp_path):
        """Test loading callbacks from empty file."""
        callbacks_file = tmp_path / "callbacks.pkl"
        callbacks_file.touch()  # Create empty file

        loaded = load_callbacks(str(callbacks_file))

        assert loaded == []

    def test_load_callbacks_missing_file(self, tmp_path, caplog):
        """Test loading callbacks when file doesn't exist."""
        import logging

        caplog.set_level(logging.WARNING)

        loaded = load_callbacks(str(tmp_path / "nonexistent.pkl"))

        assert loaded == []
        assert "Callbacks file not found" in caplog.text


class TestWorkflowBuilderModule:
    """Tests for runner.workflow_builder module."""

    def test_cluster_config_defaults(self):
        """Test ClusterConfig default values."""
        config = ClusterConfig()

        assert config.slurmfile_path is None
        assert config.env_name is None
        assert config.parent_packaging_config is None
        assert config.container_image is None
        assert config.prebuilt_images is None
        assert config.job_dir is None

    def test_cluster_config_with_values(self):
        """Test ClusterConfig with provided values."""
        config = ClusterConfig(
            slurmfile_path="/path/to/Slurmfile.toml",
            env_name="prod",
            parent_packaging_config={"type": "container", "image": "myimage"},
            container_image="myimage:latest",
            prebuilt_images={"dep1": "dep1:v1"},
            job_dir="/path/to/job",
        )

        assert config.slurmfile_path == "/path/to/Slurmfile.toml"
        assert config.env_name == "prod"
        assert config.parent_packaging_config == {
            "type": "container",
            "image": "myimage",
        }
        assert config.container_image == "myimage:latest"
        assert config.prebuilt_images == {"dep1": "dep1:v1"}
        assert config.job_dir == "/path/to/job"

    def test_extract_cluster_config_empty_env(self):
        """Test extracting config when no env vars are set."""
        # Clear relevant env vars
        env_vars = [
            "SLURM_SDK_SLURMFILE",
            "SLURM_SDK_ENV",
            "SLURM_SDK_PACKAGING_CONFIG",
            "CONTAINER_IMAGE",
            "SLURM_SDK_PREBUILT_IMAGES",
        ]
        saved = {k: os.environ.pop(k, None) for k in env_vars}

        try:
            config = extract_cluster_config_from_env()

            assert config.slurmfile_path is None
            assert config.env_name is None
            assert config.parent_packaging_config is None
            assert config.container_image is None
            assert config.prebuilt_images is None
        finally:
            # Restore env vars
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    def test_extract_cluster_config_with_slurmfile(self):
        """Test extracting config with SLURM_SDK_SLURMFILE set."""
        with patch.dict(os.environ, {"SLURM_SDK_SLURMFILE": "/path/to/Slurmfile.toml"}):
            config = extract_cluster_config_from_env()

            assert config.slurmfile_path == "/path/to/Slurmfile.toml"

    def test_extract_cluster_config_with_env_name(self):
        """Test extracting config with SLURM_SDK_ENV set."""
        with patch.dict(os.environ, {"SLURM_SDK_ENV": "production"}):
            config = extract_cluster_config_from_env()

            assert config.env_name == "production"

    def test_extract_cluster_config_with_container_image(self):
        """Test extracting config with CONTAINER_IMAGE set."""
        with patch.dict(os.environ, {"CONTAINER_IMAGE": "myregistry/myimage:v1"}):
            config = extract_cluster_config_from_env()

            assert config.container_image == "myregistry/myimage:v1"
            # Should create default packaging config for containers
            assert config.parent_packaging_config is not None
            assert config.parent_packaging_config["type"] == "container"
            assert config.parent_packaging_config["image"] == "myregistry/myimage:v1"
            assert config.parent_packaging_config["push"] is False

    def test_extract_cluster_config_with_packaging_config(self):
        """Test extracting config with encoded packaging config."""
        import base64

        pkg_config = {"type": "wheel", "path": "/path/to/wheel"}
        encoded = base64.b64encode(json.dumps(pkg_config).encode()).decode()

        with patch.dict(os.environ, {"SLURM_SDK_PACKAGING_CONFIG": encoded}):
            config = extract_cluster_config_from_env()

            assert config.parent_packaging_config == pkg_config

    def test_extract_cluster_config_with_prebuilt_images(self):
        """Test extracting config with prebuilt images."""
        import base64

        images = {"dep1": "registry/dep1:v1", "dep2": "registry/dep2:v2"}
        encoded = base64.b64encode(json.dumps(images).encode()).decode()

        with patch.dict(os.environ, {"SLURM_SDK_PREBUILT_IMAGES": encoded}):
            config = extract_cluster_config_from_env()

            assert config.prebuilt_images == images

    def test_extract_cluster_config_with_job_dir(self):
        """Test extracting config with job_dir parameter."""
        config = extract_cluster_config_from_env(job_dir="/path/to/job")

        assert config.job_dir == "/path/to/job"

    def test_extract_cluster_config_container_image_augments_packaging_config(self):
        """Test that container image augments existing packaging config."""
        import base64

        # Existing container config without image
        pkg_config = {"type": "container", "registry": "myregistry"}
        encoded = base64.b64encode(json.dumps(pkg_config).encode()).decode()

        env = {
            "SLURM_SDK_PACKAGING_CONFIG": encoded,
            "CONTAINER_IMAGE": "myregistry/myimage:v1",
        }

        with patch.dict(os.environ, env):
            config = extract_cluster_config_from_env()

            # Should have added image to the existing config
            assert config.parent_packaging_config["type"] == "container"
            assert config.parent_packaging_config["image"] == "myregistry/myimage:v1"
            assert config.parent_packaging_config["push"] is False

    def test_create_workflow_context(self, tmp_path):
        """Test creating a WorkflowContext."""
        from slurm.workflow import WorkflowContext

        job_dir = tmp_path / "job"
        job_dir.mkdir()

        ctx = create_workflow_context(
            cluster=None,
            job_id="12345",
            job_dir=str(job_dir),
        )

        assert isinstance(ctx, WorkflowContext)
        assert ctx.workflow_job_id == "12345"
        assert ctx.workflow_job_dir == job_dir
        assert ctx.shared_dir == job_dir / "shared"
        assert ctx.local_mode is False

    def test_create_workflow_context_with_defaults(self):
        """Test creating WorkflowContext with default values."""
        from slurm.workflow import WorkflowContext

        ctx = create_workflow_context(
            cluster=None,
            job_id=None,
            job_dir=None,
        )

        assert isinstance(ctx, WorkflowContext)
        assert ctx.workflow_job_id == "unknown"
        # job_dir should default to cwd

    def test_determine_parent_packaging_type_with_known_type(self):
        """Test determining packaging type when already known."""
        assert determine_parent_packaging_type("wheel") == "wheel"
        assert determine_parent_packaging_type("container") == "container"

    def test_determine_parent_packaging_type_from_venv(self):
        """Test detecting wheel packaging from VIRTUAL_ENV."""
        with patch.dict(os.environ, {"VIRTUAL_ENV": "/path/to/venv"}):
            result = determine_parent_packaging_type(None)
            assert result == "wheel"

    def test_determine_parent_packaging_type_from_singularity(self):
        """Test detecting container packaging from SINGULARITY_NAME."""
        # Clear VIRTUAL_ENV to ensure container is detected
        env = {"SINGULARITY_NAME": "mycontainer.sif"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("VIRTUAL_ENV", None)
            result = determine_parent_packaging_type(None)
            assert result == "container"

    def test_determine_parent_packaging_type_from_slurm_container(self):
        """Test detecting container packaging from SLURM_CONTAINER_IMAGE."""
        env = {"SLURM_CONTAINER_IMAGE": "registry/image:tag"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("VIRTUAL_ENV", None)
            os.environ.pop("SINGULARITY_NAME", None)
            result = determine_parent_packaging_type(None)
            assert result == "container"

    def test_determine_parent_packaging_type_none(self):
        """Test detecting no packaging when no env vars set."""
        # Clear all relevant env vars
        env_vars = ["VIRTUAL_ENV", "SINGULARITY_NAME", "SLURM_CONTAINER_IMAGE"]
        saved = {k: os.environ.pop(k, None) for k in env_vars}

        try:
            result = determine_parent_packaging_type(None)
            assert result == "none"
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    def test_workflow_setup_result_dataclass(self):
        """Test WorkflowSetupResult dataclass."""
        result = WorkflowSetupResult(
            cluster=None,
            workflow_context="mock_context",
            parent_packaging_type="wheel",
            context_token="mock_token",
        )

        assert result.cluster is None
        assert result.workflow_context == "mock_context"
        assert result.parent_packaging_type == "wheel"
        assert result.context_token == "mock_token"

    def test_cleanup_cluster_connections_with_none(self):
        """Test cleanup with None cluster doesn't raise."""
        # Should not raise any errors
        cleanup_cluster_connections(None)

    def test_cleanup_cluster_connections_without_backend(self):
        """Test cleanup with cluster without backend attribute."""

        class MockCluster:
            pass

        cluster = MockCluster()
        # Should not raise any errors
        cleanup_cluster_connections(cluster)

    def test_cleanup_cluster_connections_without_client(self):
        """Test cleanup with cluster with backend but no client."""

        class MockBackend:
            client = None

        class MockCluster:
            backend = MockBackend()

        cluster = MockCluster()
        # Should not raise any errors
        cleanup_cluster_connections(cluster)

    def test_deactivate_workflow_context_with_none(self):
        """Test deactivating with None token doesn't raise."""
        # Should not raise any errors
        deactivate_workflow_context(None)

    def test_teardown_workflow_execution(self):
        """Test full teardown process."""
        result = WorkflowSetupResult(
            cluster=None,
            workflow_context="mock_context",
            parent_packaging_type="wheel",
            context_token=None,  # No token, should handle gracefully
        )

        # Should not raise any errors
        teardown_workflow_execution(result)
