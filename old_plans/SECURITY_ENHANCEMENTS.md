# Security Enhancement Proposal

This document proposes security improvements for the SLURM SDK, organized by scope
and implementation complexity.

## Executive Summary

The SDK currently demonstrates good security practices: proper shell escaping,
credential clearing, and file permission hardening. This proposal identifies
opportunities to make the SDK **safer by default** while maintaining usability.

Key recommendations:
- Change SSH host key policy default to `reject`
- Replace pickle with safer serialization for simple types
- Add input validation at API boundaries
- Improve error handling to avoid information disclosure

---

## 1. Default Setting Changes

These changes modify default values to improve security out-of-the-box.

### 1.1 SSH Host Key Policy: `warn` → `reject`

**Current:** `host_key_policy="warn"` (accepts unknown hosts with a log warning)

**Proposed:** `host_key_policy="reject"` (refuse connections to unknown hosts)

| Aspect | Details |
|--------|---------|
| **File** | `src/slurm/api/ssh.py:56` |
| **Breaking** | Yes - users must add hosts to known_hosts first |

**Pros:**
- Prevents man-in-the-middle attacks by default
- Encourages proper SSH hygiene
- Aligns with security best practices

**Cons:**
- First-time users will get connection errors
- Requires documentation update and migration guide
- Some CI/CD environments may need configuration changes

**Migration path:**
1. Add prominent warning in 0.5.0 release notes
2. Change default in 0.6.0
3. Provide clear error message with remediation steps

**Recommendation:** ✅ Implement with migration period

---

### 1.2 Job Base Directory: `tempdir` fallback → `~/.slurm_sdk/jobs`

**Current:** Falls back to system temp directory if not configured

**Proposed:** Default to `~/.slurm_sdk/jobs` (user-specific, persistent)

| Aspect | Details |
|--------|---------|
| **File** | `src/slurm/cluster.py:1114`, `src/slurm/api/ssh.py:109` |
| **Breaking** | Minor - existing jobs in temp may be orphaned |

**Pros:**
- Avoids shared temp directory on multi-user systems
- Persistent across reboots (useful for debugging)
- Consistent location for job artifacts

**Cons:**
- Creates directory in user home (may surprise users)
- Requires cleanup strategy for old jobs

**Recommendation:** ✅ Implement

---

### 1.3 Script Permissions: `0o750` → `0o700`

**Current:** Job scripts are created with `0o750` (user rwx, group rx)

**Proposed:** Default to `0o700` (user rwx only)

| Aspect | Details |
|--------|---------|
| **File** | `src/slurm/api/local.py:36` |
| **Breaking** | May break SLURM setups requiring group access |

**Pros:**
- Prevents other users from reading job scripts
- Reduces attack surface on shared systems

**Cons:**
- Some SLURM configurations require group-readable scripts
- May require per-cluster configuration

**Recommendation:** ⚠️ Consider but document compatibility issues

---

### 1.4 SSH Password Authentication: Enabled → Discouraged

**Current:** Password authentication is a supported option

**Proposed:** Keep support but add runtime warning when passwords are used

| Aspect | Details |
|--------|---------|
| **File** | `src/slurm/api/ssh.py:89` |
| **Breaking** | No - warning only |

**Pros:**
- Nudges users toward SSH keys
- Passwords visible in process lists (security risk)
- SSH keys are more secure and convenient

**Cons:**
- Some environments require password auth
- May annoy users who intentionally use passwords

**Recommendation:** ✅ Implement as warning (not error)

---

## 2. Small to Medium Refactors

These changes improve security through code changes without major architectural shifts.

### 2.1 Hybrid Serialization: Pickle + JSON

**Problem:** Pickle can execute arbitrary code during deserialization (CWE-502)

**Current approach:** All task arguments serialized with pickle

**Proposed:** Use JSON for JSON-serializable types, pickle only when necessary

```python
# Proposed logic in rendering.py
def serialize_args(args, kwargs):
    try:
        # Try JSON first (safe)
        return {"format": "json", "data": json.dumps({"args": args, "kwargs": kwargs})}
    except (TypeError, ValueError):
        # Fall back to pickle (with warning in logs)
        logger.debug("Using pickle serialization for complex types")
        return {"format": "pickle", "data": base64.b64encode(pickle.dumps(...))}
```

| Aspect | Details |
|--------|---------|
| **Files** | `src/slurm/rendering.py`, `src/slurm/runner.py` |
| **Effort** | Medium (2-3 days) |
| **Breaking** | No - backward compatible |

**Pros:**
- JSON is safe from code execution attacks
- Reduces pickle usage to only when necessary
- Makes serialized data human-readable for debugging

**Cons:**
- Adds complexity to serialization logic
- JSON cannot serialize all Python types
- Slight performance overhead for type checking

**Recommendation:** ✅ Implement

---

### 2.2 Input Validation at API Boundaries

**Problem:** `Cluster.from_args()` accepts values with minimal validation

**Proposed:** Add validation for all user-supplied parameters using `validation.py`

```python
# In cluster.py from_args()
from slurm.validation import validate_hostname, validate_account, validate_partition

def from_args(cls, args: argparse.Namespace, **overrides) -> "Cluster":
    hostname = getattr(args, "hostname", None)
    if hostname:
        validate_hostname(hostname)  # Raises ValueError if invalid

    account = getattr(args, "account", None)
    if account:
        validate_account(account)
    ...
```

| Aspect | Details |
|--------|---------|
| **Files** | `src/slurm/cluster.py`, `src/slurm/validation.py` |
| **Effort** | Small (1 day) |
| **Breaking** | May reject previously-accepted invalid input |

**Pros:**
- Catches injection attempts early
- Provides clear error messages
- Single validation point

**Cons:**
- May be overly restrictive for edge cases
- Requires maintaining validation rules

**Recommendation:** ✅ Implement

---

### 2.3 Structured Error Handling

**Problem:** Full tracebacks exposed to users may leak internal paths

**Current:**
```python
traceback.print_exc(file=sys.stderr)  # Exposes internal structure
```

**Proposed:** Create error wrapper that limits information disclosure

```python
class SDKError(Exception):
    """Base exception with controlled information disclosure."""

    def __init__(self, message: str, details: Optional[str] = None):
        self.message = message
        self.details = details  # Only logged at DEBUG level
        super().__init__(message)

# Usage
try:
    ...
except Exception as e:
    logger.debug("Full error details: %s", traceback.format_exc())
    raise SDKError(
        "Failed to submit job. Check cluster connectivity.",
        details=str(e)
    ) from None  # Suppress chained traceback
```

| Aspect | Details |
|--------|---------|
| **Files** | `src/slurm/errors.py`, all modules raising exceptions |
| **Effort** | Medium (2-3 days) |
| **Breaking** | Changes exception types |

**Pros:**
- Reduces information leakage
- Cleaner error messages for users
- Full details available in debug logs

**Cons:**
- May hide useful debugging information
- Requires updating all error handling

**Recommendation:** ⚠️ Consider for high-security environments

---

### 2.4 Remove `execute_command()` Public API

**Problem:** `backend.execute_command(cmd)` accepts arbitrary shell commands

**Current:** Documented as public API with security warning

**Proposed:** Deprecate and eventually remove, or require explicit opt-in

```python
def execute_command(self, command: str, *, allow_arbitrary: bool = False) -> str:
    """Execute a command on the backend.

    Warning: This executes arbitrary shell commands. Only use with trusted input.

    Args:
        command: Shell command to execute
        allow_arbitrary: Must be True to execute (prevents accidental misuse)
    """
    if not allow_arbitrary:
        raise ValueError(
            "execute_command() requires allow_arbitrary=True. "
            "This API executes arbitrary shell commands and should only be used "
            "with fully trusted input."
        )
    ...
```

| Aspect | Details |
|--------|---------|
| **Files** | `src/slurm/api/local.py`, `src/slurm/api/ssh.py` |
| **Effort** | Small (1 day) |
| **Breaking** | Yes - requires code changes |

**Pros:**
- Prevents accidental shell injection
- Makes dangerous operation explicit
- Encourages use of safer alternatives

**Cons:**
- Breaks existing code using this API
- Some legitimate use cases exist

**Recommendation:** ⚠️ Add warning first, require flag in next major version

---

### 2.5 Audit Logging for Security Events

**Problem:** No centralized logging of security-relevant events

**Proposed:** Add structured audit logging for:
- SSH connection attempts (success/failure)
- Host key verification decisions
- Job submissions
- File transfers

```python
# New module: src/slurm/audit.py
import logging

audit_logger = logging.getLogger("slurm.audit")

def log_ssh_connection(hostname: str, username: str, success: bool, policy: str):
    audit_logger.info(
        "SSH connection: host=%s user=%s success=%s policy=%s",
        hostname, username, success, policy
    )

def log_job_submission(job_id: str, task_name: str, cluster: str):
    audit_logger.info(
        "Job submitted: id=%s task=%s cluster=%s",
        job_id, task_name, cluster
    )
```

| Aspect | Details |
|--------|---------|
| **Files** | New `src/slurm/audit.py`, `src/slurm/api/ssh.py` |
| **Effort** | Small (1 day) |
| **Breaking** | No |

**Pros:**
- Enables security monitoring
- Helps with compliance requirements
- Useful for debugging connection issues

**Cons:**
- Adds logging overhead
- May log sensitive information (usernames)

**Recommendation:** ✅ Implement

---

## 3. Larger Refactors

These changes require significant architectural work but provide substantial security benefits.

### 3.1 Signed Pickle Files

**Problem:** Pickle files on the cluster could theoretically be tampered with

**Proposed:** Sign pickle files with HMAC before writing, verify before loading

```python
import hmac
import hashlib

class SecurePickle:
    """Pickle wrapper with HMAC signature verification."""

    def __init__(self, secret_key: bytes):
        self.secret_key = secret_key

    def dumps(self, obj) -> bytes:
        data = pickle.dumps(obj)
        signature = hmac.new(self.secret_key, data, hashlib.sha256).digest()
        return signature + data

    def loads(self, signed_data: bytes):
        signature = signed_data[:32]
        data = signed_data[32:]
        expected = hmac.new(self.secret_key, data, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("Pickle signature verification failed")
        return pickle.loads(data)  # nosec B301 - verified signature
```

| Aspect | Details |
|--------|---------|
| **Files** | New `src/slurm/secure_pickle.py`, rendering.py, runner.py |
| **Effort** | Large (1 week) |
| **Breaking** | Yes - requires key management |

**Pros:**
- Detects tampering with serialized data
- Defense in depth against cluster compromise
- Can detect accidental file corruption

**Cons:**
- Requires secure key distribution to cluster
- Adds complexity to serialization
- Key management is difficult

**Key distribution options:**
1. Embed in environment variable (simplest)
2. Derive from SSH session key (complex but elegant)
3. User-provided secret in Slurmfile (explicit)

**Recommendation:** ⚠️ Consider for high-security environments

---

### 3.2 Sandbox Mode for Task Execution

**Problem:** Tasks run with full user privileges on the cluster

**Proposed:** Optional sandbox mode using Linux namespaces or containers

```python
@task(sandbox=True)  # Enables sandboxing
def my_task(x, y):
    return x + y
```

Sandboxing could provide:
- Network isolation (no outbound connections)
- Filesystem isolation (read-only except job directory)
- Resource limits (CPU, memory)

| Aspect | Details |
|--------|---------|
| **Files** | `src/slurm/runner.py`, new sandbox module |
| **Effort** | Very Large (2-4 weeks) |
| **Breaking** | No - opt-in feature |

**Pros:**
- Limits damage from malicious or buggy tasks
- Prevents data exfiltration
- Resource control

**Cons:**
- Significant implementation effort
- Requires kernel support (namespaces)
- May conflict with SLURM's own isolation
- Not all clusters support required features

**Recommendation:** ❌ Too complex for current scope

---

### 3.3 Replace Pickle with Custom Serialization Protocol

**Problem:** Pickle is fundamentally unsafe for untrusted data

**Proposed:** Design a custom serialization format that is safe by construction

Features:
- Whitelist of allowed types
- No code execution during deserialization
- Human-readable format (JSON-based)
- Support for common scientific types (numpy arrays, pandas DataFrames)

```python
# Proposed format
{
    "version": 1,
    "args": [
        {"type": "int", "value": 42},
        {"type": "numpy.ndarray", "dtype": "float64", "shape": [10, 10], "data": "base64..."}
    ],
    "kwargs": {
        "name": {"type": "str", "value": "example"}
    }
}
```

| Aspect | Details |
|--------|---------|
| **Files** | New serialization module, rendering.py, runner.py |
| **Effort** | Very Large (3-4 weeks) |
| **Breaking** | Yes - new serialization format |

**Pros:**
- Eliminates pickle security concerns entirely
- Explicit control over what can be serialized
- Better error messages for unsupported types

**Cons:**
- Major undertaking
- May not support all user types
- Requires fallback for complex objects
- Version compatibility concerns

**Recommendation:** ⚠️ Long-term goal, not immediate priority

---

### 3.4 Certificate-Based SSH Authentication

**Problem:** SSH key authentication relies on key files that can be stolen

**Proposed:** Support SSH certificates for short-lived authentication

```python
cluster = Cluster(
    hostname="cluster.example.com",
    ssh_certificate="/path/to/cert",  # Short-lived certificate
    ssh_certificate_authority="ca.example.com",  # For verification
)
```

| Aspect | Details |
|--------|---------|
| **Files** | `src/slurm/api/ssh.py` |
| **Effort** | Medium (1 week) |
| **Breaking** | No - new optional feature |

**Pros:**
- Certificates expire automatically
- Centralized access control
- Better audit trail

**Cons:**
- Requires PKI infrastructure
- More complex setup
- Not all clusters support certificates

**Recommendation:** ⚠️ Nice to have, low priority

---

## 4. Implementation Priority

### Phase 1: Quick Wins (1-2 weeks)

| Change | Effort | Impact |
|--------|--------|--------|
| 1.4 Password auth warning | 1 day | Low |
| 2.2 Input validation | 1 day | Medium |
| 2.5 Audit logging | 1 day | Low |
| 1.2 Job base directory | 1 day | Low |

### Phase 2: Breaking Changes (next minor release)

| Change | Effort | Impact |
|--------|--------|--------|
| 1.1 Host key policy default | 1 day | High |
| 2.4 execute_command flag | 1 day | Medium |

### Phase 3: Medium-Term (next quarter)

| Change | Effort | Impact |
|--------|--------|--------|
| 2.1 Hybrid serialization | 3 days | Medium |
| 2.3 Structured errors | 3 days | Low |

### Phase 4: Long-Term (future consideration)

| Change | Effort | Impact |
|--------|--------|--------|
| 3.1 Signed pickle | 1 week | Medium |
| 3.3 Custom serialization | 3-4 weeks | High |
| 3.4 SSH certificates | 1 week | Low |

---

## 5. Compatibility and Migration

### Breaking Changes Summary

| Change | Migration Path |
|--------|----------------|
| Host key policy default | Document in release notes, provide `host_key_policy="warn"` for old behavior |
| execute_command flag | Deprecation warning in 0.5.x, require flag in 0.6.0 |
| Job base directory | Minimal impact, document new location |

### Backward Compatibility Strategy

1. **Deprecation warnings** for changing behaviors
2. **Feature flags** to opt-in to new security features
3. **Environment variables** to restore old defaults
4. **Migration guide** for each breaking change

---

## 6. Testing Security Changes

Each security change should include:

1. **Unit tests** for the new behavior
2. **Integration tests** verifying end-to-end security
3. **Negative tests** confirming rejection of malicious input
4. **Documentation** of security implications

Example test for host key rejection:

```python
def test_reject_unknown_host():
    """Verify that reject policy refuses unknown hosts."""
    cluster = Cluster(
        hostname="unknown-host.example.com",
        host_key_policy="reject",
    )
    with pytest.raises(paramiko.ssh_exception.SSHException):
        cluster.backend._connect()
```

---

## 7. Conclusion

The SLURM SDK has a solid security foundation. The proposed enhancements focus on:

1. **Making secure defaults the norm** - Users shouldn't need to opt-in to security
2. **Defense in depth** - Multiple layers of protection
3. **Explicit over implicit** - Dangerous operations require explicit acknowledgment
4. **Auditability** - Clear logging of security-relevant events

The phased approach allows incremental improvement while maintaining compatibility
for existing users.
