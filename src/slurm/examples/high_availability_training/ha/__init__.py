"""High-availability helpers for the high_availability_training example.

These helpers are intentionally scoped to the example for now. They centralize:
- Stable run directories with atomic state updates
- Lease-based single-supervisor locking
- Standardized task result records (`result.json`)
- Optional in-job restart loops (NVRx-style wiring)

The goal is to keep train/eval task code focused on business logic.
"""

from slurm.examples.high_availability_training.ha.lock import (
    LockHeldError,
    SupervisorLeaseLock,
)
from slurm.examples.high_availability_training.ha.fluent import (
    HARuntime,
    ResultRef,
    ha_runtime,
    ha_task,
    ha_workflow,
)
from slurm.examples.high_availability_training.ha.resiliency import (
    InJobStopRequestedError,
    ResiliencyUnavailableError,
    run_with_resiliency,
)
from slurm.examples.high_availability_training.ha.task_attempt import (
    StopRequestedError,
    run_task_attempt,
)
from slurm.examples.high_availability_training.ha.task_result import (
    StatusCode,
    make_task_result,
)
from slurm.examples.high_availability_training.ha.train_eval import (
    TrainEvalSupervisor,
    TrainEvalSupervisorConfig,
)

__all__ = [
    "InJobStopRequestedError",
    "HARuntime",
    "LockHeldError",
    "ResultRef",
    "ResiliencyUnavailableError",
    "StatusCode",
    "StopRequestedError",
    "SupervisorLeaseLock",
    "TrainEvalSupervisor",
    "TrainEvalSupervisorConfig",
    "make_task_result",
    "run_task_attempt",
    "run_with_resiliency",
    "ha_runtime",
    "ha_task",
    "ha_workflow",
]
