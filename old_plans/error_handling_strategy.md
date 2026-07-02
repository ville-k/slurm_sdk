# Error Handling Strategy Design Document

**Version:** 0.3.0
**Date:** 2025-10-22
**Status:** Proposal
**Author:** Claude Code Analysis

---

## Problem Statement

The slurm-sdk currently has **inconsistent error handling** across different modules:

1. **Some methods raise exceptions:**
   - `SSHCommandBackend` raises `BackendCommandError` and `BackendTimeout`
   - `LocalBackend` raises `BackendCommandError` and `BackendTimeout`
   - Packaging errors raise `PackagingError`
   - `Cluster.__init__()` can raise various exceptions

2. **Some methods return error dictionaries:**
   - `Job.get_status()` returns `{"JobState": "UNKNOWN", "Error": str(e)}` on failure (line 217-219 in job.py)
   - Some backend methods may return error indicators

This inconsistency makes the API unpredictable and harder to use. Users don't know whether to:
- Wrap calls in try/except
- Check return values for error indicators
- Do both

---

## Current State Analysis

### Where Exceptions Are Raised

1. **Backend Operations** (`src/slurm/api/`)
   - `SSHCommandBackend._run_command()` raises `BackendCommandError`, `BackendTimeout`
   - `LocalBackend._run_command()` raises `BackendCommandError`, `BackendTimeout`
   - Connection failures raise various exceptions

2. **Packaging** (`src/slurm/packaging/`)
   - `PackagingStrategy.prepare()` can raise `PackagingError`
   - Build failures raise exceptions

3. **Submission** (`src/slurm/cluster.py`)
   - `Cluster.submit()` raises `SubmissionError` on job submission failure
   - Validation errors raise `ValueError`, `TypeError`

4. **Configuration** (`src/slurm/config.py`)
   - `load_environment()` raises `SlurmfileError`, `SlurmfileNotFoundError`, etc.

### Where Error Dicts Are Returned

1. **Job Status** (`src/slurm/job.py` line 217-219)
   ```python
   except Exception as e:
       logger.error("[%s] Error getting job status: %s", self.id, e)
       return {"JobState": "UNKNOWN", "Error": str(e)}
   ```

2. **Backend get_cluster_info** (`src/slurm/api/ssh.py` line 581)
   ```python
   except Exception as e:
       logger.warning("Warning: Failed to get cluster info: %s", e)
       return {"partitions": []}  # Return empty list instead of failing
   ```

---

## Proposed Solutions

We present three approaches, with analysis of pros/cons:

---

### **Option 1: Exception-Based (Recommended)**

**Strategy:** All errors raise exceptions. No methods return error dictionaries.

#### Design

```python
# ✅ GOOD - Clear, predictable
try:
    status = job.get_status()
    # status is always a valid dict with job state
    print(f"Job state: {status['JobState']}")
except BackendError as e:
    print(f"Failed to get status: {e}")

# ✅ GOOD - Methods either succeed or raise
try:
    result = job.get_result()
    process_result(result)
except DownloadError as e:
    print(f"Failed to download result: {e}")
except Exception as e:
    print(f"Task failed: {e}")
```

#### Changes Required

**job.py (line 217-219):**
```python
# BEFORE (inconsistent):
except Exception as e:
    logger.error("[%s] Error getting job status: %s", self.id, e)
    return {"JobState": "UNKNOWN", "Error": str(e)}

# AFTER (exception-based):
except Exception as e:
    logger.error("[%s] Error getting job status: %s", self.id, e)
    raise BackendError(f"Failed to get status for job {self.id}") from e
```

**ssh.py get_cluster_info:**
```python
# BEFORE (swallows errors):
except Exception as e:
    logger.warning("Warning: Failed to get cluster info: %s", e)
    return {"partitions": []}

# AFTER (raises or succeeds):
except Exception as e:
    logger.warning("Warning: Failed to get cluster info: %s", e)
    raise BackendError("Failed to get cluster info") from e
```

#### Pros

✅ **Predictable:** Users know to use try/except consistently
✅ **Pythonic:** Follows Python conventions (EAFP - Easier to Ask Forgiveness than Permission)
✅ **Type-safe:** Return values have consistent types
✅ **Composable:** Easy to chain operations without error checking between each step
✅ **Standard:** Most Python libraries use exceptions (requests, boto3, etc.)
✅ **Traceback:** Full stack traces aid debugging
✅ **Selective handling:** Can catch specific exceptions or let them propagate

#### Cons

❌ **Breaking change:** Existing code checking for error dicts will break
❌ **Verbose for some use cases:** Need try/except for every call that might fail
❌ **Performance:** Exception raising has overhead (though negligible for I/O operations)

#### Migration Path

```python
# v0.3.x (current - mixed)
status = job.get_status()  # May return error dict

# v0.4.0 (add warnings)
status = job.get_status()
# If it would have returned error dict, raise DeprecationWarning and exception

# v0.5.0 (remove error dicts)
try:
    status = job.get_status()  # Always raises on error
except BackendError:
    ...
```

---

### **Option 2: Result-Based (Rust-style)**

**Strategy:** Return `Result[T, E]` objects that encapsulate success or error.

#### Design

```python
from typing import Union

class Result:
    @staticmethod
    def ok(value):
        return Ok(value)

    @staticmethod
    def err(error):
        return Err(error)

class Ok:
    def __init__(self, value):
        self.value = value

    def is_ok(self) -> bool:
        return True

    def unwrap(self):
        return self.value

class Err:
    def __init__(self, error):
        self.error = error

    def is_ok(self) -> bool:
        return False

    def unwrap(self):
        raise self.error

# Usage:
result = job.get_status()
if result.is_ok():
    status = result.unwrap()
    print(f"Job state: {status['JobState']}")
else:
    print(f"Error: {result.error}")
```

#### Pros

✅ **Explicit:** Forces error handling at call site
✅ **Type-safe:** Can use type hints (`Result[Dict, BackendError]`)
✅ **No hidden control flow:** No exceptions jumping up the stack
✅ **Composable:** Can chain with `.and_then()`, `.map()`, etc.

#### Cons

❌ **Non-Pythonic:** Goes against Python idioms
❌ **Verbose:** Every call needs `.is_ok()` check
❌ **Breaking change:** Complete API rewrite
❌ **Complexity:** Requires Result type library or implementation
❌ **Unfamiliar:** Most Python developers don't expect this pattern
❌ **Migration nightmare:** All existing code breaks

#### Migration Path

This would require a v2.0.0 release with complete API rewrite. Not recommended.

---

### **Option 3: Hybrid Approach**

**Strategy:** Use exceptions for exceptional cases, return error indicators for expected failures.

#### Design

```python
# Expected failures (job not found, job still running) return indicators
status = job.get_status()  # Always succeeds, may have JobState="UNKNOWN"
if status.get("JobState") == "UNKNOWN":
    print("Job status unknown")
elif status.get("JobState") == "COMPLETED":
    result = job.get_result()  # May raise if file download fails

# Unexpected failures (network error, permission denied) raise exceptions
try:
    job = cluster.submit(my_task)(args)
except SubmissionError as e:
    print(f"Failed to submit: {e}")
```

#### Pros

✅ **Flexible:** Choose appropriate error handling for each case
✅ **Graceful degradation:** Can continue with partial information
✅ **Backward compatible:** Existing error dict code continues to work

#### Cons

❌ **Inconsistent:** Users never know which pattern to expect
❌ **Confusing:** When is something "expected" vs "exceptional"?
❌ **Complex:** Need to document which methods use which approach
❌ **Error-prone:** Easy to forget to check return values
❌ **Type confusion:** Same method might return different types

#### Example Confusion

```python
# Which of these raises exceptions?
status = job.get_status()          # Returns error dict?
result = job.get_result()          # Raises exception?
info = cluster.get_cluster_info()  # Returns empty dict?

# Answer: All three are different! This is confusing.
```

---

## Recommendation

**Choose Option 1: Exception-Based**

### Rationale

1. **Pythonic:** Follows established Python conventions
2. **Consistent:** Same pattern everywhere in the codebase
3. **Industry standard:** Matches popular libraries (requests, boto3, sqlalchemy)
4. **Clear contract:** Methods either succeed (return value) or fail (raise exception)
5. **Manageable migration:** Deprecation warnings can guide users

### Implementation Plan

#### Phase 1: v0.4.0 (Deprecation)

1. Add `BackendError` to all methods that currently return error dicts
2. Log `DeprecationWarning` when error dict would be returned
3. Update documentation to show exception-based examples
4. Add migration guide

**Example:**
```python
# job.py
def get_status(self) -> Dict[str, Any]:
    """Get job status.

    Returns:
        Dictionary with job status fields.

    Raises:
        BackendError: If status cannot be retrieved.

    .. deprecated:: 0.4.0
       Returning error dictionaries is deprecated.
       In v0.5.0, this method will raise BackendError instead.
    """
    try:
        return self.cluster.backend.get_job_status(self.id)
    except Exception as e:
        import warnings
        warnings.warn(
            "get_status() will raise BackendError in v0.5.0 instead of "
            "returning error dict. Update your code to use try/except.",
            DeprecationWarning,
            stacklevel=2
        )
        logger.error("[%s] Error getting job status: %s", self.id, e)
        return {"JobState": "UNKNOWN", "Error": str(e)}
```

#### Phase 2: v0.5.0 (Remove Error Dicts)

1. Remove all error dict return paths
2. Ensure all methods raise appropriate exceptions
3. Update all documentation and examples
4. Add migration guide in CHANGELOG.md

**Example:**
```python
# job.py
def get_status(self) -> Dict[str, Any]:
    """Get job status.

    Returns:
        Dictionary with job status fields.

    Raises:
        BackendError: If status cannot be retrieved.
    """
    try:
        return self.cluster.backend.get_job_status(self.id)
    except Exception as e:
        logger.error("[%s] Error getting job status: %s", self.id, e)
        raise BackendError(f"Failed to get status for job {self.id}") from e
```

#### Phase 3: Documentation

Update all examples in docs:

**Before:**
```python
status = job.get_status()
if "Error" in status:
    print(f"Error: {status['Error']}")
else:
    print(f"State: {status['JobState']}")
```

**After:**
```python
try:
    status = job.get_status()
    print(f"State: {status['JobState']}")
except BackendError as e:
    print(f"Error: {e}")
```

---

## Exception Hierarchy

Define clear exception hierarchy:

```python
# src/slurm/errors.py

class SlurmSDKError(Exception):
    """Base exception for all slurm-sdk errors."""
    pass

# Backend errors
class BackendError(SlurmSDKError):
    """Base class for backend-related errors."""
    pass

class BackendTimeout(BackendError, TimeoutError):
    """Backend operation timed out."""
    pass

class BackendCommandError(BackendError):
    """Backend command failed."""
    pass

# Job errors
class JobError(SlurmSDKError):
    """Base class for job-related errors."""
    pass

class SubmissionError(JobError):
    """Job submission failed."""
    pass

class DownloadError(JobError):
    """Failed to download job results."""
    pass

# Packaging errors
class PackagingError(SlurmSDKError):
    """Packaging operation failed."""
    pass

# Configuration errors
class SlurmfileError(SlurmSDKError):
    """Slurmfile-related error."""
    pass
```

Users can catch at any level:
```python
try:
    job = cluster.submit(task)()
    result = job.get_result()
except SubmissionError:
    # Handle submission failures
    pass
except DownloadError:
    # Handle download failures
    pass
except BackendError:
    # Handle any backend error
    pass
except SlurmSDKError:
    # Catch all slurm-sdk errors
    pass
```

---

## Decision Record

**Decision:** Adopt **Option 1: Exception-Based** error handling

**Date:** 2025-10-22

**Rationale:**
- Most Pythonic approach
- Consistent with industry standards
- Clear migration path
- Better developer experience

**Next Steps:**
1. Implement Phase 1 (deprecation) in v0.4.0
2. Document migration guide
3. Implement Phase 2 (removal) in v0.5.0
4. Update all documentation and examples

---

## Appendix: Code Examples

### Exception Handling Best Practices

```python
# ✅ GOOD: Specific exception handling
try:
    job = cluster.submit(my_task)(42)
    result = job.wait(timeout=300).get_result()
except SubmissionError as e:
    logger.error(f"Submission failed: {e}")
    # Maybe retry with different cluster
except BackendTimeout:
    logger.warning("Job timed out, checking status...")
    # Maybe extend timeout
except BackendError as e:
    logger.error(f"Backend error: {e}")
    # Fatal error, can't recover
```

```python
# ✅ GOOD: Re-raise with context
try:
    job = cluster.submit(my_task)(args)
except SubmissionError as e:
    raise RuntimeError(f"Failed to process item {item_id}") from e
```

```python
# ❌ BAD: Catching too broad
try:
    job = cluster.submit(my_task)(42)
except Exception:  # Too broad!
    pass
```

```python
# ❌ BAD: Silent failures
try:
    result = job.get_result()
except:
    pass  # User won't know what went wrong
```

---

## References

- [PEP 3134: Exception Chaining](https://www.python.org/dev/peps/pep-3134/)
- [Python Exception Handling Best Practices](https://realpython.com/python-exceptions/)
- [Effective Python: Item 68 - Use Exceptions to Indicate Special Situations](https://effectivepython.com/)
- Comparison with similar libraries:
  - `requests`: Exception-based (ConnectionError, Timeout, HTTPError)
  - `boto3`: Exception-based (ClientError, BotoCoreError)
  - `paramiko`: Exception-based (SSHException, AuthenticationException)
  - `sqlalchemy`: Exception-based (SQLAlchemyError, OperationalError)
