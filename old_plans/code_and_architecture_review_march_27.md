# SLURM SDK Code & Architecture Review — March 27, 2026

**Scope**: Full review of `src/slurm/` (22,900 LOC across 85 files) and `tests/` (13,400 LOC across 58 files).

---

## 1. Bugs

### 1.1 Temp file leak in `Job.get_result()` (SSH path)

**File**: `job.py:501-575`

`get_result()` creates a `NamedTemporaryFile(delete=False)` for SSH downloads but has no `finally` block to clean it up. Compare with `_read_remote_file()` at line 858-860, which correctly cleans up via `finally: os.unlink(local_temp_path)`.

If the download fails, the pickle load raises, or the DownloadError is raised, the temp file persists on the local filesystem. Over many result retrievals, this accumulates orphan files in `/tmp`.

### 1.2 Thread-unsafe status cache in `Job`

**File**: `job.py:167-188, 229-231`

`_status_cache`, `_status_cache_time`, and `_completed` are read and written without synchronization. The `_JobStatusPoller` background thread calls `job._update_status_cache()` (cluster.py:78) concurrently with user-side `get_status()` calls.

A thread could read `_status_cache_time` (updated) but see a stale `_status_cache` (not yet written), or vice versa. The `_completed` flag has the same issue — it's set inside `_update_status_cache` but checked without locks in `is_completed()` and `wait()`.

The existing `_completed_context_lock` only guards callback emission (cluster.py:1884), not status reads.

### 1.3 Slurmfile modification via string manipulation

**File**: `cluster.py:1504-1544`

Slurmfile (TOML) content is modified by line-by-line string matching (`stripped.startswith("hostname")`) instead of proper TOML parsing. This breaks with:

- Inline comments: `hostname = "foo" # old host`
- Quoted keys: `"hostname" = "foo"`
- Multiline values or table arrays
- Keys containing `hostname` as a prefix (e.g., `hostname_alias`)

The SDK already depends on `tomli` for reading TOML — consider using `tomlkit` (preserves formatting) for round-trip modification.

### 1.4 `_job_pollers` dict accessed without synchronization

**File**: `cluster.py:1865-1870, 1930-1933`

`_job_pollers` is a plain dict modified from the main thread (`_maybe_start_job_poller`) and from background poller threads (`_on_poller_finished`). Dict mutations from concurrent threads can cause `RuntimeError: dictionary changed size during iteration` or silent data corruption.

---

## 2. Code Quality Issues

### 2.1 Job name not validated before SBATCH directive

**File**: `rendering.py:143-146`

```python
job_name = sbatch_params.pop("job_name", None)
if not job_name:
    job_name = task_func.__name__
script_lines.append(f"#SBATCH --job-name={job_name}")
```

The `validation.py` module provides `validate_job_name()`, and other parameters like `output`/`error` paths use `shlex.quote()`, but `job_name` is interpolated directly. While SBATCH directives aren't shell-interpreted in the same way, a job name containing newlines or `#` could inject additional directives.

### 2.2 `shell=True` for string commands in `LocalBackend`

**File**: `api/local.py:112-128`

```python
use_shell = isinstance(cmd, str)
result = subprocess.run(cmd, shell=use_shell, ...)  # nosec B603 B602
```

String commands are dispatched with `shell=True` for backward compatibility. All internal callers pass lists (safe), but `execute_command()` is a public API that accepts strings. The `nosec` suppression hides this from static analysis.

### 2.3 SSH host key policy defaults to "warn"

**File**: `api/ssh.py:149-162`

Default `host_key_policy="warn"` accepts unknown host keys with a log warning. This is convenient for development but inappropriate for production. The `auto` policy is even less secure. The default should be `reject` with clear documentation on how to opt into less-secure modes.

### 2.4 Callback exceptions silently swallowed

**File**: `cluster.py:1034-1044` and throughout

All callback invocations catch `Exception` and log at `debug` level:

```python
except Exception as exc:
    logger.debug("Callback %s failed in on_begin_package_ctx: %s", ...)
```

A broken callback is invisible to users unless they enable debug logging. At minimum, this should be `logger.warning()` so users know their callback has a bug.

### 2.5 `datetime.utcnow()` is deprecated

**File**: `_runner_impl.py:216`

```python
"created_at": datetime.utcnow().isoformat() + "Z",
```

`datetime.utcnow()` was deprecated in Python 3.12. Use `datetime.now(datetime.UTC)` instead.

### 2.6 Environment metadata written with default permissions

**File**: `_runner_impl.py:220-221`

```python
with open(metadata_path, "w") as f:
    json.dump(metadata, f, indent=2)
```

The metadata file may include environment variable snapshots (all of `os.environ`), which could contain credentials. The file is created with the default umask (often `0o644`, world-readable). Should use restricted permissions (e.g., `0o600`) or filter sensitive env vars before writing.

### 2.7 Inconsistent quoting in rendered scripts

**File**: `rendering.py:143-174`

Output and error paths use `shlex.quote()`, but job name and other SBATCH parameters do not. The quoting strategy should be uniform — either validate all parameters at the boundary or quote them all in the renderer.

### 2.8 Backend method detection via `hasattr` on private methods

**File**: `cluster.py:1553-1556`

```python
if hasattr(self.backend, "_upload_string_to_file"):
    self.backend._upload_string_to_file(modified_content, remote_slurmfile_path)
```

Duck-typing on private methods (`_upload_string_to_file`) is fragile. If a backend renames or removes the method, the code silently falls through to a different path. This should be a proper method on `BackendBase` or use an explicit `is_remote()` check.

### 2.9 Cluster context leak on `__enter__` failure

**File**: `cluster.py:2044-2078`

If `_set_active_context` succeeds in `__enter__` but subsequent code before the `with` body raises, `__exit__` is still called (Python guarantees this). However, if the Cluster constructor has issues and someone manually calls `__enter__`, a late failure could leave a stale context token on `self._context_token` without cleanup.

### 2.10 `object.__new__(Cluster)` in tests bypasses `__init__`

**File**: Many test files

Tests create Cluster instances via `object.__new__(Cluster)` then manually set attributes. This bypasses `__init__` validation and silently breaks if `__init__` adds required attributes. A proper test factory or fixture would be more maintainable.

---

## 3. Architectural Issues

### 3.1 `cluster.py` is a 2,200-line monolith

`cluster.py` handles:
- Slurmfile loading and parsing
- Backend initialization
- Packaging orchestration
- Job script rendering orchestration
- Job submission (including two-phase submit)
- Workflow Slurmfile generation and upload
- Job status polling thread management
- Completed-context callback emission
- Job metadata writing
- Job retrieval and reconstruction
- Context manager protocol
- Cluster diagnostics

This violates single responsibility. Extracting packaging orchestration, workflow management, and polling into separate modules would reduce cognitive load and make testing easier.

### 3.2 `Job` tightly coupled to `Cluster`

`Job.__init__` takes a `Cluster` instance and calls `self.cluster.backend.*` directly for all operations (status, cancel, download). This makes it impossible to test a `Job` without a fully constructed `Cluster` and means job objects hold a reference to the entire cluster (and transitively, the SSH connection).

A thinner interface (e.g., a "backend handle" or protocol) would decouple these and allow lighter-weight job objects.

### 3.3 No abstraction for remote file operations

File upload, download, temp directory creation, and file reading are scattered across `SSHCommandBackend`, `LocalBackend`, `Cluster`, and `Job`. Some operations check for specific backend methods (`_upload_string_to_file`), others use `is_remote()`, and still others isinstance-check against `SSHCommandBackend`.

A unified file operations interface on `BackendBase` (with `upload_file`, `download_file`, `read_file`, `write_file`) would eliminate the isinstance checks and duck-typing throughout the codebase.

### 3.4 Pickle as the sole serialization format

Results, array items, and task arguments all use pickle. This creates:
- **Cross-version fragility**: Python version mismatches between client and cluster break silently.
- **No human-readable inspection**: Can't check a result file without Python.
- **Security surface**: While the SDK controls serialization, any path traversal or file replacement attack yields arbitrary code execution.

Consider supporting JSON for simple types with pickle as a fallback, or at minimum, writing a version header to pickle files to catch version mismatches early.

### 3.5 Callback dispatch is inline and repetitive

Every callback invocation follows the same pattern:
```python
for callback in self.callbacks:
    if not callback.should_run_on_client("on_X_ctx"):
        continue
    try:
        callback.on_X_ctx(context)
    except Exception as exc:
        logger.debug(...)
```

This 6-line block appears ~10 times in `cluster.py`. A `_dispatch_callback(event_name, context)` helper would reduce duplication and centralize error handling / logging level decisions.

### 3.6 Two separate runner implementations

`_runner_impl.py` (626 lines) coexists with the `runner/` package (9 files, 2,135 lines). The `main()` function in `_runner_impl.py` is the actual entry point but imports from `runner/` for modular components. The `runner/__init__.py` re-exports everything from both.

This split is confusing — `_runner_impl.py` should either be folded into `runner/main.py` or clearly documented as a legacy entry point being phased out.

### 3.7 Packaging strategy `prepare()` does too much

`ContainerPackagingStrategy.prepare()` handles building, tagging, pushing to registry, uploading to cluster, and generating setup commands — all in one method. This makes it difficult to:
- Cache intermediate results (e.g., skip rebuild if image exists)
- Test individual steps
- Report granular progress

A pipeline pattern (build → tag → push → upload) with explicit intermediate states would improve testability and observability.

### 3.8 No connection pooling or lifecycle management

`SSHCommandBackend` creates a single SSH connection in `__init__` and reuses it for the lifetime of the backend. There's no:
- Reconnection on stale connections
- Connection health checking
- Explicit close/cleanup (no `close()` method on `BackendBase`)

The `Cluster` context manager (`__exit__`) resets the contextvars token but doesn't close the backend connection.

---

## 4. Test Coverage Gaps

### Critical gaps (zero or near-zero coverage):

| Area | Issue |
|------|-------|
| SSH backend | No unit tests for `SSHCommandBackend` (connection, retry, timeout, SFTP operations) |
| Concurrent access | No tests for thread safety of `Job` status cache or `_job_pollers` dict |
| Slurmfile round-trip | No tests for TOML modification (the string-based parser in cluster.py:1504) |
| Large scale | No tests with >100 array tasks or concurrent submissions |
| Error recovery | Limited testing of cleanup after mid-operation failures (partial uploads, interrupted downloads) |
| Security | No tests for input sanitization (job names with special chars, path traversal, env var injection) |

### Test anti-patterns:

- **`object.__new__(Cluster)` bypasses init**: ~20 test files construct Cluster without calling `__init__`, making tests fragile if constructor changes.
- **Pragma no-cover on threading code**: `_JobStatusPoller.run()` and poller cleanup are excluded from coverage, yet these are where the thread-safety bugs live.
- **Integration test gating**: Integration tests require Docker + env vars + `/etc/hosts` entries, making them impractical to run in most environments.

---

## 5. Summary of Priorities

### Fix now (bugs):
1. Add `finally: os.unlink(local_temp_path)` to `Job.get_result()` SSH path
2. Add locking around `_status_cache` / `_completed` in `Job`
3. Use proper TOML library for Slurmfile modification
4. Synchronize `_job_pollers` dict access

### Fix soon (quality):
5. Validate job names in `render_job_script()` before emitting SBATCH directives
6. Elevate callback exception logging from `debug` to `warning`
7. Replace `datetime.utcnow()` with `datetime.now(datetime.UTC)`
8. Restrict permissions on environment metadata file
9. Eliminate `shell=True` from `LocalBackend._run_command()` for string inputs

### Address incrementally (architecture):
10. Extract packaging orchestration and polling from `cluster.py`
11. Unify file operations on `BackendBase` to eliminate isinstance/hasattr checks
12. Add `close()` to backend interface; call it from `Cluster.__exit__`
13. Extract callback dispatch into a helper method
14. Reconcile `_runner_impl.py` and `runner/` package structure

---

## 6. Remaining work (as of PR #24)

PR #24 (`refactors_and_fixes`) addressed all 4 bugs, 9 of 10 quality issues, and 5 of 8 architectural items. The items below remain open because they are large design-level changes that need direction before implementation.

### 6.1 Unresolved code quality issue

**2.9 — Cluster context leak on manual `__enter__`**: If someone calls `cluster.__enter__()` directly without a `with` block and an exception occurs before they call `__exit__()`, the context token leaks. This is an edge case with minimal real-world risk — the `with` statement guarantees cleanup. Left as-is unless manual context management becomes a documented pattern.

### 6.2 Architectural items needing design decisions

#### 3.1 — `cluster.py` decomposition

`cluster.py` is still ~2,000 lines handling packaging orchestration, workflow slurmfile management, submission, polling, and diagnostics. Splitting it would improve readability and testability.

**Design questions:**
- What module boundaries make sense? Options: (a) extract polling into `polling.py`, packaging orchestration into `packaging_orchestrator.py`, workflow slurmfile handling into `workflow.py`; (b) group by lifecycle phase (submit, monitor, retrieve); (c) keep Cluster as a thin facade delegating to internal modules.
- Should the extracted modules be public API or private implementation details?

#### 3.2 — `Job` tightly coupled to `Cluster`

`Job` holds a reference to the full `Cluster` (and transitively, the SSH connection) and calls `self.cluster.backend.*` directly. This makes `Job` impossible to test without a Cluster and creates unnecessarily long-lived references.

**Design questions:**
- Should `Job` take a protocol/interface (e.g., `JobBackend` with `get_status`, `cancel`, `download_file`) instead of the full Cluster?
- Would this break the ergonomic `job.get_result()` API that users rely on?
- Should `Job` become a lightweight data object with operations moved to Cluster methods (e.g., `cluster.get_result(job)`)?

#### 3.4 — Pickle as sole serialization format

All results, array items, and task arguments use pickle. This is fragile across Python versions and opaque to inspect.

**Design questions:**
- Is cross-version execution (e.g., Python 3.11 client submitting to 3.12 cluster) a real use case that needs support now?
- Preferred approach: (a) add a version header to pickle files for early mismatch detection, (b) support JSON for simple types with pickle fallback, (c) use `cloudpickle` for better cross-version support?
- Is human-readable result inspection a priority (e.g., for debugging failed jobs)?

#### 3.7 — Packaging `prepare()` does too much

`ContainerPackagingStrategy.prepare()` builds, tags, pushes, and uploads in one method. This prevents caching, granular progress reporting, and step-level testing.

**Design questions:**
- Should this become a pipeline with explicit stages (build → tag → push → upload), or is a simpler refactor (extract private methods) sufficient?
- Is image caching (skip rebuild if image exists) a priority feature, or is the current always-rebuild behavior acceptable?
- Should the pipeline stages be exposed as public API for advanced users?

#### 3.8 — SSH connection lifecycle

`close()` was added to `BackendBase` and `Cluster.__exit__` now calls it. But there is still no reconnection on stale connections or health checking.

**Design questions:**
- How common are long-lived Cluster objects that outlive their SSH connection (e.g., interactive notebooks)?
- Should reconnection be transparent (auto-retry on failure) or explicit (user calls `cluster.reconnect()`)?
- Is connection health checking worth the overhead, or is lazy reconnection on failure sufficient?

### 6.3 Test coverage gaps

These are not blocked on design decisions — they can be tackled incrementally:

- **SSH backend unit tests**: Zero direct tests for `SSHCommandBackend`. Needs paramiko mocking strategy.
- **Thread contention tests**: The new locks are in place but untested under actual concurrent access.
- **Scale tests**: No tests with >100 array tasks or concurrent submissions.
- **`pragma: no cover` on threading code**: `_JobStatusPoller.run()` is excluded from coverage — the thread-safety fixes live there.
