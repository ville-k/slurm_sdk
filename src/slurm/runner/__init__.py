"""Runner package for executing Slurm jobs.

This package contains the internal runner script executed by Slurm jobs
to run Python tasks.
"""

# Re-export main entry point from the implementation module
from slurm._runner_impl import main

# Also export from new modules (public API)
from slurm.runner.argument_loader import (
    RunnerArgs,
    configure_logging,
    create_argument_parser,
    load_callbacks,
    load_task_arguments,
    log_startup_info,
    parse_args,
    restore_sys_path,
)
from slurm.runner.callbacks import run_callbacks
from slurm.runner.context_manager import (
    bind_workflow_context,
    function_wants_workflow_context,
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
from slurm.runner.workflow_builder import (
    ClusterConfig,
    WorkflowSetupResult,
    activate_workflow_context,
    cleanup_cluster_connections,
    create_cluster_from_config,
    create_workflow_context,
    deactivate_workflow_context,
    determine_parent_packaging_type,
    extract_cluster_config_from_env,
    setup_workflow_execution,
    teardown_workflow_execution,
)
from slurm.runner.main import (
    execute_task,
    get_environment_snapshot,
    get_job_id_from_env,
    handle_job_context_injection,
    handle_workflow_context_injection,
    load_function,
    run_task_with_callbacks,
)

__all__ = [
    # Main entry point
    "main",
    # Public API
    "run_callbacks",
    "function_wants_workflow_context",
    "bind_workflow_context",
    "write_environment_metadata",
    "save_result",
    "update_job_metadata",
    "resolve_placeholder",
    "resolve_task_arguments",
    # Argument parsing and loading
    "RunnerArgs",
    "create_argument_parser",
    "parse_args",
    "configure_logging",
    "log_startup_info",
    "restore_sys_path",
    "load_task_arguments",
    "load_callbacks",
    # Workflow builder
    "ClusterConfig",
    "WorkflowSetupResult",
    "extract_cluster_config_from_env",
    "create_cluster_from_config",
    "create_workflow_context",
    "determine_parent_packaging_type",
    "activate_workflow_context",
    "setup_workflow_execution",
    "cleanup_cluster_connections",
    "deactivate_workflow_context",
    "teardown_workflow_execution",
    # Main orchestration
    "get_job_id_from_env",
    "get_environment_snapshot",
    "load_function",
    "execute_task",
    "handle_job_context_injection",
    "handle_workflow_context_injection",
    "run_task_with_callbacks",
]
