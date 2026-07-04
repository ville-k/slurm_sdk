"""Argument parsing and loading utilities for the runner.

This module handles parsing command-line arguments and loading task
arguments from serialized files.
"""

import argparse
import base64
import logging
import os

# nosec B403 - pickle is required for deserializing task arguments and results
# Security note: pickle files are created by the SDK and transferred via trusted SSH/local storage
import pickle
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple, cast

from slurm.callbacks import BaseCallback

logger = logging.getLogger("slurm.runner")


@dataclass
class RunnerArgs:
    """Parsed runner command-line arguments.

    This dataclass holds all the arguments passed to the runner script,
    supporting both regular jobs and array jobs.
    """

    # Required arguments
    module: str
    function: str
    output_file: str
    callbacks_file: str

    # Regular job arguments (optional, mutually exclusive with array job args)
    args_file: Optional[str] = None
    kwargs_file: Optional[str] = None

    # Array job arguments (optional, mutually exclusive with regular job args)
    array_index: Optional[int] = None
    array_items_file: Optional[str] = None

    # Optional arguments
    sys_path: Optional[str] = None
    loglevel: str = "INFO"
    job_dir: Optional[str] = None
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    pre_submission_id: Optional[str] = None

    # Parallel-step dispatch. ``step`` is a selector like ``"peer:<name>"``
    # or ``"peer:<name>:by-taskid"``. For plain peer steps it is
    # informational — args come from ``--args-file`` / ``--kwargs-file``
    # just like regular jobs, so the peer path reuses
    # :func:`_load_regular_task_arguments`.
    step: Optional[str] = None

    @property
    def is_array_job(self) -> bool:
        """Check if this is an array job based on arguments."""
        return self.array_index is not None

    @property
    def is_peer_step(self) -> bool:
        """Check if this invocation runs a peer step of a ``parallel(...)`` job."""
        return self.step is not None and self.step.startswith("peer:")

    @property
    def peer_name(self) -> Optional[str]:
        """Return the peer name parsed from ``--step peer:<name>[:...]``."""
        if not self.is_peer_step:
            return None
        # step format: "peer:<name>" or "peer:<name>:by-taskid"
        assert self.step is not None
        parts = self.step.split(":", 2)
        if len(parts) >= 2 and parts[1]:
            return parts[1]
        return None

    @property
    def is_replica_peer_step(self) -> bool:
        """Whether this invocation is a ``:by-taskid`` replica dispatch.

        Replica peers run one ``srun --ntasks=N`` step. Each of the N Slurm
        tasks inside that step invokes the runner with the same
        ``--step peer:<name>:by-taskid`` selector; the runner then reads
        ``SLURM_PROCID`` to pick its replica's args file.
        """
        if not self.is_peer_step:
            return False
        assert self.step is not None
        parts = self.step.split(":", 2)
        return len(parts) == 3 and parts[2] == "by-taskid"


_REPLICA_RESULT_MARKER = "_result.pkl"


def apply_replica_output_suffix(output_file: str, replica_index: int) -> str:
    """Rewrite ``output_file`` to include the replica index.

    In ``:by-taskid`` mode, every replica in an ``srun --ntasks=N`` step
    runs this same runner binary with the same ``--output-file`` arg. If
    we did not rewrite the filename per replica, all N tasks would race
    onto the same pickle and ``ParallelJob``'s per-replica ``Job`` handles
    (built in ``_parallel_submission.py`` as ``<peer_pre_id>_<idx>``) would
    read garbage — or, worse, read whatever the last replica to finish
    happened to write.

    The transform inserts ``_replica<index>`` immediately before the
    canonical ``_result.pkl`` suffix so the emitted filename matches what
    :meth:`Job._result_filename` expects (``_parallel_submission`` builds
    replica pre-submission ids with the same ``_replica<index>`` segment).
    The explicit marker keeps replica files disjoint from singleton peers
    whose *name* happens to end in ``_<digit>``.

    ``slurm_job_foo_result.pkl`` + ``replica=3`` →
    ``slurm_job_foo_replica3_result.pkl``

    Non-canonical filenames (shouldn't happen in practice but we guard
    against them) get the index appended as a trailing segment so nothing
    silently collides.
    """
    if not output_file.endswith(_REPLICA_RESULT_MARKER):
        return f"{output_file}.replica{replica_index}"
    base = output_file[: -len(_REPLICA_RESULT_MARKER)]
    return f"{base}_replica{replica_index}{_REPLICA_RESULT_MARKER}"


def resolve_replica_output_file(args: "RunnerArgs") -> str:
    """Return the runner's per-replica ``output_file`` for replica steps.

    Callers outside ``:by-taskid`` mode get ``args.output_file`` unchanged.
    Replica dispatch reads ``SLURM_PROCID`` (set by Slurm per task) and
    rewrites the path so each replica writes to its own pickle.
    """
    if not args.is_replica_peer_step:
        return args.output_file
    procid_raw = os.environ.get("SLURM_PROCID", "0")
    try:
        procid = int(procid_raw)
    except ValueError:
        procid = 0
    return apply_replica_output_suffix(args.output_file, procid)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the runner script.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(description="Slurm Task Runner")

    # Required arguments
    parser.add_argument(
        "--module", required=True, help="Module containing the task function"
    )
    parser.add_argument("--function", required=True, help="Name of the task function")
    parser.add_argument(
        "--output-file", required=True, help="Path to save the pickled result"
    )
    parser.add_argument(
        "--callbacks-file",
        required=True,
        help="Path to the pickled callbacks list file",
    )

    # Regular job arguments (mutually exclusive with array job arguments)
    parser.add_argument(
        "--args-file", help="Path to the pickled args tuple file (regular jobs)"
    )
    parser.add_argument(
        "--kwargs-file", help="Path to the pickled kwargs dict file (regular jobs)"
    )

    # Array job arguments (mutually exclusive with regular job arguments)
    parser.add_argument(
        "--array-index", type=int, help="Array task index for native SLURM arrays"
    )
    parser.add_argument(
        "--array-items-file", help="Path to pickled array items file (array jobs)"
    )

    # Optional arguments
    parser.add_argument(
        "--sys-path", help="Original sys.path pickled and base64 encoded"
    )
    parser.add_argument("--loglevel", help="Log level", default="INFO")
    parser.add_argument("--job-dir", help="Resolved job directory on the runner")
    parser.add_argument("--stdout-path", help="Scheduler stdout path for the job")
    parser.add_argument("--stderr-path", help="Scheduler stderr path for the job")
    parser.add_argument(
        "--pre-submission-id",
        help="SDK pre-submission identifier associated with this run",
    )
    parser.add_argument(
        "--step",
        help=(
            "Parallel-step selector, e.g. 'peer:<name>'. Set by the "
            "supervisor for steps inside a parallel(...) allocation."
        ),
    )
    return parser


def parse_args(argv: Optional[list[str]] = None) -> RunnerArgs:
    """Parse command-line arguments into a RunnerArgs dataclass.

    Args:
        argv: Command-line arguments to parse. If None, uses sys.argv.

    Returns:
        RunnerArgs dataclass with parsed arguments.
    """
    parser = create_argument_parser()
    args = parser.parse_args(argv)

    return RunnerArgs(
        module=args.module,
        function=args.function,
        output_file=args.output_file,
        callbacks_file=args.callbacks_file,
        args_file=args.args_file,
        kwargs_file=args.kwargs_file,
        array_index=args.array_index,
        array_items_file=args.array_items_file,
        sys_path=args.sys_path,
        loglevel=args.loglevel,
        job_dir=args.job_dir,
        stdout_path=args.stdout_path,
        stderr_path=args.stderr_path,
        pre_submission_id=args.pre_submission_id,
        step=args.step,
    )


def configure_logging(loglevel: str) -> None:
    """Configure logging with the specified level.

    Args:
        loglevel: Log level string (e.g., "INFO", "DEBUG").
    """
    logging.basicConfig(level=loglevel)


def log_startup_info(args: RunnerArgs) -> None:
    """Log startup information about the runner.

    Args:
        args: Parsed runner arguments.
    """
    logger.info("=" * 70)
    logger.info("RUNNER STARTING")
    logger.info(f"Module: {args.module}, Function: {args.function}")
    logger.info(f"Job Dir: {args.job_dir}")
    logger.info("=" * 70)

    if args.is_array_job:
        logger.info("Starting array task execution (index=%s)", args.array_index)
        logger.debug("Module=%s, Function=%s", args.module, args.function)
        logger.debug("Array index=%s", args.array_index)
        logger.debug("Array items file=%s", args.array_items_file)
        logger.debug("Output file=%s", args.output_file)
        logger.debug("Callbacks file=%s", args.callbacks_file)
    elif args.is_peer_step:
        logger.info(
            "Starting peer step execution (step=%s, peer=%s, replica=%s)",
            args.step,
            args.peer_name,
            args.is_replica_peer_step,
        )
        logger.debug("Module=%s, Function=%s", args.module, args.function)
        logger.debug("Args file=%s", args.args_file)
        logger.debug("Kwargs file=%s", args.kwargs_file)
        logger.debug("Output file=%s", args.output_file)
        logger.debug("Callbacks file=%s", args.callbacks_file)
    else:
        logger.info("Starting task execution")
        logger.debug("Module=%s, Function=%s", args.module, args.function)
        logger.debug("Args file=%s", args.args_file)
        logger.debug("Kwargs file=%s", args.kwargs_file)
        logger.debug("Output file=%s", args.output_file)
        logger.debug("Callbacks file=%s", args.callbacks_file)
    logger.debug("Log level=%s", args.loglevel)


def restore_sys_path(encoded_sys_path: str) -> None:
    """Restore sys.path from base64-encoded pickle.

    This helps find user modules when running in the Slurm environment.
    Original paths are prepended to ensure user modules are found first.

    Args:
        encoded_sys_path: Base64-encoded pickled sys.path list.
    """
    from .._serialization import loads_pickled

    original_sys_path = cast(
        List[str], loads_pickled(base64.b64decode(encoded_sys_path.encode()))
    )
    # Prepend original paths to ensure user modules are found first
    sys.path = original_sys_path + [p for p in sys.path if p not in original_sys_path]
    logger.debug("Updated sys.path: %s", sys.path)


def load_task_arguments(
    args: RunnerArgs, job_dir: Optional[str] = None
) -> Tuple[tuple, dict]:
    """Load task arguments from serialized files.

    Handles both regular jobs (args/kwargs files) and array jobs (items file).

    Args:
        args: Parsed runner arguments.
        job_dir: Job directory for resolving relative paths.

    Returns:
        Tuple of (task_args, task_kwargs).
    """
    if args.is_array_job:
        return _load_array_task_arguments(args, job_dir)
    elif args.is_replica_peer_step:
        return _load_peer_replica_arguments(args, job_dir)
    elif args.is_peer_step:
        return _load_peer_task_arguments(args, job_dir)
    else:
        return _load_regular_task_arguments(args)


def _load_regular_task_arguments(args: RunnerArgs) -> Tuple[tuple, dict]:
    """Load arguments for a regular (non-array) job.

    Args:
        args: Parsed runner arguments.

    Returns:
        Tuple of (task_args, task_kwargs).
    """
    from .._serialization import read_pickled

    assert args.args_file is not None
    assert args.kwargs_file is not None
    task_args = cast(tuple, read_pickled(args.args_file))
    task_kwargs = cast(dict, read_pickled(args.kwargs_file))
    return task_args, task_kwargs


def _load_array_task_arguments(
    args: RunnerArgs, job_dir: Optional[str] = None
) -> Tuple[tuple, dict]:
    """Load arguments for an array job task.

    Args:
        args: Parsed runner arguments.
        job_dir: Job directory for resolving relative paths.

    Returns:
        Tuple of (task_args, task_kwargs).
    """
    from slurm.array_items import load_array_item

    assert args.array_items_file is not None
    assert args.array_index is not None

    # Resolve array items file path
    # If it's a relative path, resolve it relative to job_dir
    array_items_path = args.array_items_file
    if not os.path.isabs(array_items_path):
        # Relative path - resolve relative to job directory
        if job_dir:
            array_items_path = os.path.join(job_dir, array_items_path)
        else:
            # Fallback to current directory
            array_items_path = os.path.abspath(array_items_path)

    logger.debug(
        "Loading array item at index %s from %s",
        args.array_index,
        array_items_path,
    )
    logger.debug("Resolved array items path: %s", array_items_path)
    item = load_array_item(array_items_path, args.array_index)

    # Unpack item based on type
    if isinstance(item, dict):
        # Dict: unpack as kwargs
        task_args = ()
        task_kwargs = item
        logger.debug("Loaded dict item as kwargs: %s keys", len(task_kwargs))
    elif isinstance(item, tuple):
        # Tuple: unpack as args
        task_args = item
        task_kwargs = {}
        logger.debug("Loaded tuple item as args: %s elements", len(task_args))
    else:
        # Single value: pass as first arg
        task_args = (item,)
        task_kwargs = {}
        logger.debug("Loaded single item as first arg")

    return task_args, task_kwargs


def _load_peer_task_arguments(
    args: RunnerArgs, job_dir: Optional[str] = None
) -> Tuple[tuple, dict]:
    """Load arguments for a peer step inside a ``parallel(...)`` allocation.

    Peer args are serialized to per-peer files and passed via the standard
    ``--args-file`` / ``--kwargs-file`` flags, so this helper reuses
    :func:`_load_regular_task_arguments` — it exists to give the dispatch
    branch in :func:`load_task_arguments` an obvious hook for peer-specific
    argument loading (e.g. registry lookup) should it ever diverge.

    Paths are resolved relative to ``job_dir`` when not absolute, mirroring the
    array-item helper.

    Args:
        args: Parsed runner arguments.
        job_dir: Job directory for resolving relative paths.

    Returns:
        Tuple of (task_args, task_kwargs).
    """
    from .._serialization import read_pickled

    if args.args_file is None or args.kwargs_file is None:
        raise RuntimeError(
            "Peer step invocation is missing --args-file or --kwargs-file. "
            "This is an internal error in the parallel rendering pipeline."
        )

    args_path = args.args_file
    kwargs_path = args.kwargs_file
    if job_dir:
        if not os.path.isabs(args_path):
            args_path = os.path.join(job_dir, args_path)
        if not os.path.isabs(kwargs_path):
            kwargs_path = os.path.join(job_dir, kwargs_path)

    logger.debug(
        "Loading peer args from %s, kwargs from %s (peer=%s)",
        args_path,
        kwargs_path,
        args.peer_name,
    )
    task_args = cast(tuple, read_pickled(args_path))
    task_kwargs = cast(dict, read_pickled(kwargs_path))
    return task_args, task_kwargs


def _load_peer_replica_arguments(
    args: RunnerArgs, job_dir: Optional[str] = None
) -> Tuple[tuple, dict]:
    """Load per-replica args for a ``:by-taskid`` peer step dispatch.

    The replica index is read from ``SLURM_PROCID`` (0..count-1 within the
    step). The per-replica pickle lives at ``peer_<name>_replica<i>_args.pkl``
    in ``$JOB_DIR`` — written once per replica at submission time so the
    runner only has to select and deserialise.

    The pickle contains a single value whose type drives unpacking: ``dict``
    becomes ``kwargs``, ``tuple`` becomes positional args, anything else is
    passed as the first positional arg (same shape as array-item unpacking).

    Args:
        args: Parsed runner arguments — ``step == "peer:<name>:by-taskid"``.
        job_dir: Job directory for resolving relative paths.

    Returns:
        ``(task_args, task_kwargs)`` ready for ``execute_task``.
    """
    from .._serialization import read_pickled
    from slurm.array_items import unpack_item_to_args_kwargs

    peer_name = args.peer_name
    if not peer_name:
        raise RuntimeError(
            "Replica peer step invocation has no peer name; the runner "
            "expected '--step peer:<name>:by-taskid'."
        )

    # SLURM_PROCID is the per-step task id Slurm assigns to each of the N
    # tasks spawned by ``srun --ntasks=N``. Tests that exercise the loader
    # out-of-band can set ``SLURM_PROCID`` manually.
    procid_raw = os.environ.get("SLURM_PROCID")
    if procid_raw is None:
        raise RuntimeError(
            "Replica peer step invocation requires SLURM_PROCID to select "
            "the replica's args file. The supervisor / parallel renderer "
            "should set this via ``srun --ntasks=<count>``."
        )
    try:
        replica_index = int(procid_raw)
    except ValueError as exc:
        raise RuntimeError(
            f"SLURM_PROCID={procid_raw!r} is not a valid integer."
        ) from exc
    if replica_index < 0:
        raise RuntimeError(f"SLURM_PROCID={replica_index} must be non-negative.")

    from slurm.parallel.plan import PEER_REPLICA_ARGS_BASENAME

    args_filename = PEER_REPLICA_ARGS_BASENAME.format(
        name=peer_name, index=replica_index
    )
    args_path = args_filename
    if job_dir and not os.path.isabs(args_path):
        args_path = os.path.join(job_dir, args_path)

    logger.debug(
        "Loading replica args for peer=%s replica=%s from %s",
        peer_name,
        replica_index,
        args_path,
    )

    item = read_pickled(args_path)
    task_args, task_kwargs = unpack_item_to_args_kwargs(item)
    return task_args, task_kwargs


def load_callbacks(callbacks_file: str) -> List[BaseCallback]:
    """Load callbacks from a pickled file.

    Handles empty files (no callbacks) and various error conditions.

    Args:
        callbacks_file: Path to the pickled callbacks file.

    Returns:
        List of callback instances. Empty list if no callbacks or on error.
    """
    callbacks: List[BaseCallback] = []
    try:
        with open(callbacks_file, "rb") as f:
            # Handle empty file case (created by rendering script if no callbacks)
            content = f.read()
            if content:
                from .._serialization import loads_pickled

                callbacks = cast(List[BaseCallback], loads_pickled(content))
                logger.debug("Deserialized %d callbacks.", len(callbacks))
            else:
                logger.debug("No callbacks provided (empty callbacks file).")
    except FileNotFoundError:
        logger.warning("Runner: Callbacks file not found at %s", callbacks_file)
    except (pickle.UnpicklingError, EOFError) as e:
        logger.error(
            "Runner: Error deserializing callbacks from %s: %s",
            callbacks_file,
            e,
        )
    return callbacks


__all__ = [
    "RunnerArgs",
    "create_argument_parser",
    "parse_args",
    "configure_logging",
    "log_startup_info",
    "restore_sys_path",
    "load_task_arguments",
    "load_callbacks",
    "_load_peer_task_arguments",
    "_load_peer_replica_arguments",
]
