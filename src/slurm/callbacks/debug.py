"""Debug callback for attaching debugpy to SLURM jobs."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict

from .callbacks import BaseCallback, ExecutionLocus, RunBeginContext

logger = logging.getLogger(__name__)


class DebugCallback(BaseCallback):
    """Callback that enables debugpy debugging for SLURM jobs.

    This callback sets up debugpy to listen for debugger connections when the job
    starts on the runner. It can be configured to wait for a debugger to attach
    before continuing execution.

    Configuration in Slurmfile:

        [[tool.slurm.callbacks]]
        target = "slurm.callbacks:DebugCallback"
        port = 5678
        wait_for_client = false
        host = "0.0.0.0"

    Or via environment variables:

        DEBUGPY_PORT=5678
        DEBUGPY_WAIT=1
        DEBUGPY_HOST=0.0.0.0

    Usage with CLI:

        # Submit job with debug callback
        # Then use: slurm jobs debug <job-id>

    The callback only runs on the RUNNER side to set up the debugger.
    """

    execution_loci: Dict[str, ExecutionLocus] = {
        "on_begin_run_job_ctx": ExecutionLocus.RUNNER,
    }

    requires_pickling: bool = True

    def __init__(
        self,
        *,
        port: int = 5678,
        wait_for_client: bool = False,
        host: str = "0.0.0.0",  # nosec B104 - debugpy requires binding to all interfaces for remote debugging
        log_connection_info: bool = True,
    ) -> None:
        """Initialize the debug callback.

        Args:
            port: Port for debugpy to listen on. Defaults to 5678.
            wait_for_client: If True, block execution until debugger attaches.
            host: Host address to bind to. Defaults to all interfaces.
            log_connection_info: If True, log connection details at startup.
        """
        self.port = port
        self.wait_for_client = wait_for_client
        self.host = host
        self.log_connection_info = log_connection_info

    def on_begin_run_job_ctx(self, ctx: RunBeginContext) -> None:
        """Set up debugpy when the job starts on the runner."""
        env_port = os.environ.get("DEBUGPY_PORT")
        env_wait = os.environ.get("DEBUGPY_WAIT")
        env_host = os.environ.get("DEBUGPY_HOST")

        port = int(env_port) if env_port else self.port
        wait_for_client = bool(env_wait) if env_wait else self.wait_for_client
        host = env_host if env_host else self.host

        try:
            import debugpy
        except ImportError:
            logger.warning(
                "debugpy not installed. Debug callback disabled. "
                "Install with: pip install debugpy"
            )
            return

        try:
            debugpy.listen((host, port))
            hostname = ctx.hostname or os.environ.get("HOSTNAME", "unknown")

            if self.log_connection_info:
                logger.info(f"debugpy listening on {host}:{port}")
                logger.info(f"Compute node: {hostname}")
                logger.info(
                    f"Connect with: slurm jobs debug {ctx.job_id} "
                    f"or configure VS Code Remote Attach to {hostname}:{port}"
                )

            if wait_for_client:
                logger.info("Waiting for debugger to attach...")
                debugpy.wait_for_client()
                logger.info("Debugger attached, continuing execution")

        except Exception as e:
            logger.error(f"Failed to start debugpy: {e}")

    def __getstate__(self) -> Dict[str, Any]:
        """Prepare state for pickling."""
        return {
            "port": self.port,
            "wait_for_client": self.wait_for_client,
            "host": self.host,
            "log_connection_info": self.log_connection_info,
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Restore state after unpickling."""
        self.port = state.get("port", 5678)
        self.wait_for_client = state.get("wait_for_client", False)
        self.host = state.get("host", "0.0.0.0")  # nosec B104 - debugpy requires binding to all interfaces
        self.log_connection_info = state.get("log_connection_info", True)
