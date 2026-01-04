"""Example tasks and workflows for the Slurm SDK.

This package contains example code demonstrating SDK features and
integration test utilities.

User-Facing Examples:
    - hello_world.py: Basic task submission example
    - hello_container.py: Container-based task example
    - hello_torch.py: PyTorch-based task example
    - map_reduce.py: Array jobs with map/reduce patterns
    - parallelization_patterns.py: Various parallelization strategies
    - workflow_graph_visualization.py: Workflow dependency visualization

Integration Test Utilities:
    The following modules are included in the package wheel to support
    integration testing. They are used by tasks and workflows that need
    to be importable when running remotely on Slurm clusters. These are
    NOT user-facing examples:

    - integration_test_task.py: Simple tasks for integration tests
    - integration_test_workflow.py: Workflows for integration tests
    - container_test_functions.py: Container packaging test tasks

    These modules remain in the examples directory because they must be
    part of the installed package to be importable by jobs running on
    remote clusters. Moving them to the tests directory would break this
    requirement since test files are not included in the wheel.
"""
