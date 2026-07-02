# GPT-5.5 Codebase Review

Reviewed the `src/slurm` package, representative tests, and documentation for bugs, performance issues, architecture, test/documentation gaps, and API fluency. This is a source review, not an exhaustive audit.

## Validation Performed

- `uv run ty check` - passed.
- `uv run pytest tests/test_backend_tail.py tests/test_workflow_slurmfile_generation.py tests/test_cluster_from_env.py -q` - passed, 11 tests.
- Reproduced one local workflow Slurmfile failure with a direct `Cluster.from_env()` check: a local Slurmfile containing `[cluster.backend_config] hostname = "..."; port = 22` raises `TypeError: LocalBackend.__init__() got an unexpected keyword argument 'hostname'`.

## High Priority Findings

### 1. Workflow Slurmfile generation can make local workflow rehydration fail

**Area:** bug, architecture, tests

`handle_workflow_slurmfile()` always writes `backend_config.hostname` and `backend_config.port` into the workflow Slurmfile (`src/slurm/_workflow.py:360-370`). `Cluster.from_env()` passes all `backend_config` keys through to backend construction (`src/slurm/cluster.py:389-443`). That is valid for the SSH backend, but invalid for the local backend, whose constructor only accepts `job_base_dir`, `env`, `timeout`, and `script_permissions` (`src/slurm/api/local.py:42-48`).

The current test only exercises a forced SSH-like path (`tests/test_workflow_slurmfile_generation.py:31-63`), so it verifies the added hostname/port but not the local workflow case. In practice, a workflow runner that reconstructs a local backend from this generated Slurmfile can fail before doing any work.

**Impact:** Local workflow orchestration is fragile. A backend-neutral config file is mutated with SSH-specific settings, and backend construction does not filter incompatible keys.

**Recommendation:** Only inject `hostname`/`port` when the generated workflow Slurmfile will reconstruct an SSH backend. Alternatively, make Slurmfile loading backend-aware and filter or validate keys before calling `create_backend()`. Add a regression test that round-trips a generated local workflow Slurmfile through `Cluster.from_env()`.

### 2. `snapshot()` and non-following `tail_file()` can hang forever before log files exist

**Area:** bug, performance, API behavior

`Job.snapshot()` calls `_read_tail()` for stdout and stderr (`src/slurm/job.py:1007-1043`). `_read_tail()` calls `backend.tail_file(..., follow=False)` and expects it to either return content or raise (`src/slurm/job.py:1062-1088`).

Both concrete backends wait indefinitely for a missing file even when `follow=False`:

- Local backend loops until `os.path.exists(path)` becomes true (`src/slurm/api/local.py:914-925`).
- SSH backend emits `while [ ! -f path ]; do sleep 1; done; tail -n ...` even when `follow=False` (`src/slurm/api/ssh.py:1073-1080`).

The focused tests pass because `tests/test_backend_tail.py:56-81` explicitly codifies waiting for a future file in `follow=False` mode. There is no test that a point-in-time snapshot of a pending job returns promptly when stdout/stderr have not yet been created.

**Impact:** A dashboard, CLI, or monitoring loop that calls `snapshot()` for a pending job can block forever on missing logs. This undermines the "point-in-time snapshot" API shape and can stall status UIs.

**Recommendation:** Treat `follow=False` as a point-in-time read: return no lines or raise `FileNotFoundError` immediately when the file is absent. Keep wait-for-file behavior only for `follow=True`, and honor `stop_event`/timeouts while waiting. Add tests for missing files with `follow=False`, plus a `Job.snapshot()` test where stdout/stderr paths do not yet exist.

### 3. SSH backend interpolates user-controlled identifiers into shell commands

**Area:** security, bug, API boundary validation

Several SSH backend methods build remote shell commands with unquoted public inputs:

- `scontrol show job {job_id}` (`src/slurm/api/ssh.py:525-529`)
- `sacct -j {job_id} ...` (`src/slurm/api/ssh.py:599-604`)
- `sacct -A {account} -S {start_time} -E {end_time} ...` (`src/slurm/api/ssh.py:657-661`)
- `scancel {job_id}` (`src/slurm/api/ssh.py:693-706`)

These values can come from public surfaces such as CLI/MCP calls, and the remote execution primitive is a shell string. The local backend generally uses argv lists, so the two backends have different safety properties.

**Impact:** A malicious or malformed job id/account/date can change the remote command. Even when the caller is trusted, accidental whitespace or shell metacharacters can produce confusing failures.

**Recommendation:** Add centralized validation for Slurm identifiers and date/account fields, allowing known valid formats such as normal jobs and array jobs. For values that cannot be strictly validated, quote with `shlex.quote()` at the command boundary. Prefer a shared command-building layer that makes SSH and local backends behave consistently. Add tests that unsafe IDs are rejected and that valid Slurm array/job-step IDs still work.

## Medium Priority Findings

### 4. Paramiko command execution can deadlock or time out on large stdout/stderr

**Area:** performance, reliability

`SSHBackend._run_command()` calls `recv_exit_status()` before reading stdout and stderr (`src/slurm/api/ssh.py:271-282`). Paramiko channels can hang when remote output exceeds the SSH window because the process cannot finish until the client drains the stream. The socket/channel timeout reduces the blast radius, but it converts a volume-dependent deadlock into a timeout for commands that should have succeeded.

**Impact:** Large `sacct`, `squeue`, diagnostic, or build outputs can make otherwise healthy commands fail unpredictably. This is especially relevant for account-level queries and cluster-wide listings.

**Recommendation:** Drain stdout/stderr while the process is running, using a channel receive loop, `select`, or a proven helper that reads both streams before waiting on final status. Add a fake-channel unit test that simulates output larger than the window and verifies `_run_command()` drains before waiting for exit status.

### 5. Regular task dependency placeholders do not support nested structures, unlike array jobs

**Area:** API fluency, bug, tests

`SlurmTask.__call__()` only replaces top-level `Job` positional and keyword arguments with `JobResultPlaceholder` (`src/slurm/task.py:302-320`). Array job item conversion already has a recursive helper for tuples, dicts, and lists (`src/slurm/array_items.py:14-61`). The regular-task gap is known in the test suite but not enforced: `tests/test_dependencies.py:244-274` comments out the nested task calls because Jobs inside nested structures cannot currently be pickled.

**Impact:** The API is inconsistent. A user can pass nested job references through array items, but `task({"value": upstream_job})` or `task([job1, job2])` can fail or serialize backend objects unexpectedly. This is surprising for workflow composition.

**Recommendation:** Extract the recursive conversion logic into a shared helper and use it in `SlurmTask.__call__()` for both args and kwargs, while collecting dependencies. Replace the commented-out assertions with active tests for nested lists, tuples, and dicts in regular tasks.

### 6. Shell rendering is not systematically quoted for job directories and cleanup paths

**Area:** bug, architecture, API robustness

Script rendering quotes some values carefully, but not consistently:

- Job scripts emit `cd $JOB_DIR` unquoted (`src/slurm/rendering.py:649-659`).
- Container mount generation renders `$(dirname $(dirname {job_dir_expr}))` without quoting the inner expression (`src/slurm/packaging/container.py:369-374`).
- Wheel cleanup emits `rm -rf {venv_path}` and `rm -f {target_wheel_path}` without a shared quoting contract (`src/slurm/packaging/wheel.py:412-423`).

**Impact:** A `job_base_dir` or generated path containing spaces, glob characters, or other shell-significant characters can break job startup or cleanup. This class of bug tends to appear only on user-specific cluster mount points.

**Recommendation:** Centralize shell expression rendering for paths and command fragments. Treat path values as data until the final render step, and add tests that submit/render jobs under a base directory containing spaces and shell metacharacters.

### 7. Wheel packaging leaks local temporary directories

**Area:** performance, resource management

`WheelPackaging.prepare()` creates a temporary directory with `tempfile.mkdtemp(prefix="slurm_wheel_")` (`src/slurm/packaging/wheel.py:81-82`) and records it in `prepare_result` (`src/slurm/packaging/wheel.py:169-187`), but there is no obvious cleanup path for the local temp directory after upload or local submission.

**Impact:** Repeated submissions can accumulate `/tmp/slurm_wheel_*` directories and wheel artifacts. This is minor for small packages but can become noticeable for active development loops or large projects.

**Recommendation:** Use `TemporaryDirectory` where lifecycle permits, or add an explicit cleanup hook after successful upload/submission. If artifacts must survive for debugging, make that an opt-in setting and document it.

### 8. Test doubles bypass important behavior in the real local backend

**Area:** testing gap, architecture

Many tests use `tests/helpers/local_backend.py`, a minimal backend that executes scripts directly and marks array jobs completed without running array behavior (`tests/helpers/local_backend.py:10-81`). There are real local backend tests elsewhere, but workflow and dependency tests using this helper do not exercise the real `src/slurm/api/local.py` command path, log files, status parsing, or array execution semantics.

**Impact:** The tests are fast, but they can validate SDK orchestration against behavior that users never run. This weakens coverage for local workflow rehydration, tailing, array jobs, and result handling.

**Recommendation:** Keep the helper for narrow unit tests, but add a small number of end-to-end tests against `slurm.api.local.LocalBackend` for workflow submission, result retrieval, array item handling, and log snapshot behavior.

## Lower Priority Findings

### 9. `@task` documentation and runtime behavior disagree about local calls

**Area:** documentation, API fluency

The public `task()` docstring says the decorated function can still be called locally (`src/slurm/decorators.py:137-142`) and shows `train_model(...)  # Runs locally` (`src/slurm/decorators.py:229-237`). The actual `SlurmTask.__call__()` behavior submits inside a cluster/workflow context and raises outside one, with `.unwrapped()` as the local-call API (`src/slurm/task.py:264-281`, `src/slurm/task.py:342-361`).

**Impact:** New users may try the documented local call and get a runtime error. The `.unwrapped` API is reasonable, but the docs need to be consistent everywhere.

**Recommendation:** Update decorator docs and examples to use `task_fn.unwrapped(...)` for local execution, or intentionally change `__call__()` to run locally outside context. The former is lower risk.

### 10. Type-checking coverage is weaker around the decorator API than the passing check implies

**Area:** testing gap, API fluency

`uv run ty check` passes, but the configuration still has relaxed rules and test overrides (`pyproject.toml:90-100`). The decorator API relies on overloads and runtime wrapper behavior, yet most tests exercise runtime behavior rather than static user experience.

**Impact:** A core selling point of a decorator-based SDK is that user functions remain ergonomic and type-checkable. The current check can miss regressions in inferred task call signatures or `.unwrapped` behavior.

**Recommendation:** Add static type expectation tests for the public decorator API, either with `ty` fixtures checked in CI or with a small sample project under tests. Promote relaxed rules incrementally as noisy issues are resolved.

## Architectural Themes

- Backend-specific concerns leak into generic workflow code. The workflow Slurmfile issue is the strongest example: SSH transport details are written into a backend-neutral config path.
- Shell command construction is scattered across SSH backend methods, script rendering, packaging strategies, and parallel rendering. A small command/path rendering abstraction would reduce quoting bugs, injection risks, and backend divergence.
- Public APIs are close to fluent, but the edges are inconsistent: nested dependencies work in one path but not another, and local testing is `.unwrapped()` even where docs imply direct calls.
- The project has useful focused tests, but several tests encode current behavior that is harmful for point-in-time APIs, and some high-level tests rely on simplified doubles instead of the real local backend.

## Suggested Next Fix Order

1. Fix workflow Slurmfile backend config injection and add a local round-trip regression test.
1. Make `follow=False` tailing nonblocking for missing files, then cover `Job.snapshot()` on pending jobs.
1. Validate or quote SSH command inputs at the boundary.
1. Rework SSH command execution to drain streams while waiting.
1. Share recursive dependency placeholder conversion between array and regular tasks.
