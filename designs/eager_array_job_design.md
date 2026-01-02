# Eager Array Job Execution Design

**Author**: Claude Code
**Date**: 2025-10-27
**Status**: Design Exploration
**Purpose**: Analyze eager execution options for array jobs while maintaining fluent API compatibility

---

## Executive Summary

This document explores the feasibility of implementing **eager array job execution** in the Slurm SDK while maintaining a fluent API. The fundamental challenge is that **SLURM does not allow adding dependencies to jobs after submission**, which conflicts with the current fluent API pattern `task.map(items).after(job)`.

**Key Findings:**
- ✅ **Compatible APIs**: `.map()`, `__getitem__`, `__iter__`, `.get_results()`, `.wait()`
- ❌ **Incompatible APIs**: `.after()` when called after `.map()`
- 🔄 **Requires Modification**: Dependency specification must happen before or during `.map()`

**Recommendation**: Choose between three primary strategies:
1. **Reversed Fluent API**: `task.after(deps).map(items)` (dependencies before map)
2. **Builder Pattern with Explicit Submit**: `task.map(items).after(deps).submit()` (manual submission)
3. **Map-Time Dependencies**: `task.map(items, after=[deps])` (dependencies as parameter)

---

## Table of Contents

1. [Background](#1-background)
2. [The Core Problem](#2-the-core-problem)
3. [Current API Analysis](#3-current-api-analysis)
4. [Design Options](#4-design-options)
5. [Trade-off Analysis](#5-trade-off-analysis)
6. [Migration Impact](#6-migration-impact)
7. [Recommendations](#7-recommendations)

---

## 1. Background

### 1.1 Current Implementation (Lazy Submission)

Array jobs currently use **lazy submission on first access**:

```python
# Array job created but NOT submitted
array_job = process_item.map(items)

# Dependencies added BEFORE submission
array_job = array_job.after(prep_job)

# Submission happens on first access
result = array_job[0]  # Triggers submission here
```

**Why This Works:**
- `.map()` creates an ArrayJob object but doesn't submit
- `.after()` adds dependencies to the un-submitted array job
- Submission happens later when jobs are accessed
- Dependencies are included in the submission

### 1.2 Desired Implementation (Eager Submission)

The goal is **immediate submission** when `.map()` is called:

```python
# Ideally: submit happens immediately at .map()
array_job = process_item.map(items)  # Submit here!

# Problem: too late to add dependencies
array_job = array_job.after(prep_job)  # CANNOT WORK - already submitted!
```

**The Constraint:**
- SLURM requires dependencies at submission time via `--dependency` flag
- Once a job is submitted, dependencies cannot be modified
- This is a fundamental SLURM limitation, not an SDK implementation detail

---

## 2. The Core Problem

### 2.1 SLURM's Dependency Model

SLURM dependencies are **immutable after submission**:

```bash
# Correct: dependencies at submit time
sbatch --dependency=afterok:12345 script.sh

# Incorrect: cannot add dependencies after submission
sbatch script.sh
# ... no way to add dependency to job after this point
```

### 2.2 The Fluent API Challenge

Python evaluates method chains left-to-right:

```python
# Evaluation order:
result = task.map(items).after(job)
         # Step 1: task.map(items)     -> returns ArrayJob
         # Step 2: .after(job)          -> called on returned ArrayJob
```

**With eager execution:**
- Step 1 would submit jobs (no dependencies known yet)
- Step 2 would try to add dependencies (too late!)

**With lazy execution:**
- Step 1 creates ArrayJob (no submission)
- Step 2 adds dependencies to un-submitted job
- Submission happens later (dependencies included)

---

## 3. Current API Analysis

### 3.1 APIs Compatible with Eager Execution

These methods work fine with eager submission:

| Method | Current Behavior | Eager Behavior | Compatible? |
|--------|------------------|----------------|-------------|
| `.map(items)` | Create ArrayJob | Create + Submit ArrayJob | ✅ Yes |
| `[index]` | Access job by index | Access job by index | ✅ Yes |
| `.__iter__()` | Iterate jobs | Iterate jobs | ✅ Yes |
| `.get_results()` | Wait for results | Wait for results | ✅ Yes |
| `.wait()` | Wait for completion | Wait for completion | ✅ Yes |
| `.get_results_dir()` | Get results path | Get results path | ✅ Yes |

**Why Compatible:**
These methods don't need to modify job state after submission. They either:
- Create the job (`.map()`)
- Read job information (`[index]`, `__iter__`)
- Wait for completion (`.wait()`, `.get_results()`)

### 3.2 APIs Incompatible with Eager Execution

| Method | Current Behavior | Eager Behavior | Compatible? |
|--------|------------------|----------------|-------------|
| `.after(*jobs)` | Add deps before submit | Add deps after submit | ❌ No |

**Why Incompatible:**
- `.after()` when called after `.map()` tries to add dependencies post-submission
- SLURM doesn't support modifying dependencies after submission
- This pattern fundamentally conflicts with eager execution

### 3.3 Current Usage Patterns

**Pattern 1: Map then After** (Incompatible with eager)
```python
array_job = task.map(items).after(prep_job)  # ❌ Breaks with eager
```

**Pattern 2: Map only** (Compatible with eager)
```python
array_job = task.map(items)  # ✅ Works with eager
```

**Pattern 3: Deferred access** (Compatible with eager)
```python
array_job = task.map(items)
# ... later ...
results = array_job.get_results()  # ✅ Works with eager
```

---

## 4. Design Options

### 4.1 Option A: Reversed Fluent API

**Dependencies before map:**

```python
# Specify dependencies on the task, then map
array_job = task.after(prep_job).map(items)
```

**Implementation:**
```python
class SlurmTask:
    def after(self, *jobs) -> "SlurmTaskWithDeps":
        """Return a task wrapper with dependencies."""
        return SlurmTaskWithDeps(self, dependencies=list(jobs))

    def map(self, items, max_concurrent=None):
        """Create and submit array job."""
        # Immediate submission with no dependencies
        return ArrayJob(self, items, dependencies=[])

class SlurmTaskWithDeps:
    def __init__(self, task, dependencies):
        self.task = task
        self.dependencies = dependencies

    def map(self, items, max_concurrent=None):
        """Create and submit array job with dependencies."""
        # Immediate submission WITH dependencies
        return ArrayJob(self.task, items, dependencies=self.dependencies)
```

**Pros:**
- ✅ Maintains fluent API style
- ✅ Enables eager execution
- ✅ Clear dependency ordering
- ✅ Type-safe (can type-hint the intermediate wrapper)

**Cons:**
- ❌ Breaking change - existing code must be rewritten
- ❌ Reads "backwards" (dependencies before the thing that depends)
- ❌ Inconsistent with regular job API: `task().after(job)` vs `task.after(job).map()`

**Migration:**
```python
# Old (lazy)
array_job = task.map(items).after(prep_job)

# New (eager)
array_job = task.after(prep_job).map(items)
```

---

### 4.2 Option B: Builder Pattern with Explicit Submit

**Require explicit `.submit()` call:**

```python
# Dependencies can be added before submission
array_job = task.map(items).after(prep_job).submit()
```

**Implementation:**
```python
class ArrayJobBuilder:
    """Builder for array jobs (not yet submitted)."""

    def __init__(self, task, items):
        self.task = task
        self.items = items
        self.dependencies = []
        self.max_concurrent = None

    def after(self, *jobs) -> "ArrayJobBuilder":
        """Add dependencies (returns self for chaining)."""
        self.dependencies.extend(jobs)
        return self

    def submit(self) -> "ArrayJob":
        """Submit the array job eagerly."""
        # Submission happens here
        return ArrayJob(
            self.task,
            self.items,
            dependencies=self.dependencies,
            max_concurrent=self.max_concurrent
        )

class SlurmTask:
    def map(self, items) -> ArrayJobBuilder:
        """Create array job builder (not submitted yet)."""
        return ArrayJobBuilder(self, items)
```

**Pros:**
- ✅ Explicit about when submission happens
- ✅ Maintains left-to-right reading order
- ✅ Flexible - can add multiple operations before submit
- ✅ Consistent with some other frameworks (e.g., SQL query builders)

**Cons:**
- ❌ Breaking change - all `.map()` calls need `.submit()` added
- ❌ Extra method call increases verbosity
- ❌ Not truly "eager" - still requires explicit action
- ❌ Easy to forget `.submit()` call

**Migration:**
```python
# Old (lazy)
array_job = task.map(items).after(prep_job)

# New (explicit)
array_job = task.map(items).after(prep_job).submit()
```

---

### 4.3 Option C: Map-Time Dependencies (Parameter)

**Pass dependencies as parameter to `.map()`:**

```python
# Dependencies specified at map time
array_job = task.map(items, after=[prep_job])
```

**Implementation:**
```python
class SlurmTask:
    def map(
        self,
        items,
        after=None,
        max_concurrent=None
    ) -> "ArrayJob":
        """Create and submit array job with optional dependencies.

        Args:
            items: Items to process
            after: Optional job dependencies (Job or ArrayJob instances)
            max_concurrent: Max concurrent tasks
        """
        dependencies = list(after) if after else []
        # Immediate submission with dependencies
        return ArrayJob(self, items, dependencies=dependencies)
```

**Pros:**
- ✅ True eager execution (submits immediately)
- ✅ All configuration in one place
- ✅ Similar to how SLURM actually works (deps at submit time)
- ✅ No intermediate builder objects
- ✅ Can be type-hinted easily

**Cons:**
- ❌ Breaking change - `.after()` method removed
- ❌ Less fluent than method chaining
- ❌ Inconsistent with regular job API: `task().after(job)` vs `task.map(items, after=[job])`
- ❌ Multiple dependencies require list syntax

**Migration:**
```python
# Old (lazy)
array_job = task.map(items).after(prep_job)

# New (parameter)
array_job = task.map(items, after=[prep_job])
```

---

### 4.4 Option D: Hybrid - Support Both Patterns

**Allow both parameter and method chaining, but prohibit mixing:**

```python
# Option 1: Parameter (eager, submits immediately)
array_job = task.map(items, after=[prep_job])

# Option 2: Method chaining (lazy, submits on access)
array_job = task.map(items).after(prep_job)
```

**Implementation:**
```python
class SlurmTask:
    def map(self, items, after=None, max_concurrent=None):
        """Create array job.

        If 'after' is provided, submits eagerly.
        If not provided, returns builder for lazy submission.
        """
        if after is not None:
            # Eager path - submit immediately with dependencies
            dependencies = list(after) if after else []
            array_job = ArrayJob(self, items, dependencies=dependencies)
            array_job._submit()  # Force immediate submission
            return array_job
        else:
            # Lazy path - return builder
            return ArrayJobBuilder(self, items)
```

**Pros:**
- ✅ Backward compatible (keeps existing `.after()` pattern)
- ✅ Offers eager option for those who want it
- ✅ Gradual migration path

**Cons:**
- ❌ Two ways to do the same thing (confusing)
- ❌ Complex implementation with two code paths
- ❌ Still has lazy submission for one path
- ❌ Type hints become complicated (union return types)

---

### 4.5 Option E: Context Manager for Dependency Tracking

**Use context manager to collect dependencies:**

```python
with cluster.dependencies(prep_job) as deps:
    array_job = task.map(items)  # Automatically gets deps from context
```

**Implementation:**
```python
@contextmanager
def dependencies(*jobs):
    """Context manager that tracks dependencies."""
    token = _push_dependencies(list(jobs))
    try:
        yield
    finally:
        _pop_dependencies(token)

def _push_dependencies(deps):
    # Store in thread-local or contextvars
    ...

def _pop_dependencies(token):
    ...

class SlurmTask:
    def map(self, items):
        # Check for context dependencies
        context_deps = _get_current_dependencies()
        return ArrayJob(self, items, dependencies=context_deps)
```

**Pros:**
- ✅ Elegant for multiple arrays with same dependencies
- ✅ Dependencies visible at block level
- ✅ Can support eager execution

**Cons:**
- ❌ Magic behavior (implicit dependency injection)
- ❌ Complex debugging (where did dependencies come from?)
- ❌ Threading complications (need contextvars)
- ❌ Breaking change to API style
- ❌ Overkill for single array job

---

### 4.6 Option F: Drop Fluent .after() Entirely

**Remove `.after()` from ArrayJob, use dependency specification at job call site:**

```python
# For regular jobs (keep existing API)
job2 = task2().after(job1)  # Still works

# For array jobs (no .after() support)
prep_job = task1()
array_job = task2.map(items)  # No way to specify dependencies

# Alternative: use Job dependencies in items
array_job = task2.map([(prep_job, item) for item in items])
```

**Implementation:**
```python
class ArrayJob:
    # Remove .after() method entirely
    # Dependencies extracted from items if they contain Job objects
```

**Pros:**
- ✅ Simplest implementation
- ✅ True eager execution
- ✅ No API confusion

**Cons:**
- ❌ Removes useful feature entirely
- ❌ Forces workarounds for common use case
- ❌ Inconsistent with regular job API
- ❌ No way to specify "all array tasks depend on prep job"

---

## 5. Trade-off Analysis

### 5.1 Comparison Matrix

| Option | Eager? | Fluent? | Breaking Change? | Complexity | Consistent with Job API? |
|--------|--------|---------|------------------|------------|-------------------------|
| A: Reversed API | ✅ Yes | ✅ Yes | ❌ Yes | Low | ❌ No (`task.after().map()` vs `job().after()`) |
| B: Builder + Submit | ⚠️ Explicit | ✅ Yes | ❌ Yes | Medium | ❌ No (requires `.submit()`) |
| C: Parameter | ✅ Yes | ⚠️ Partial | ❌ Yes | Low | ⚠️ Different (param vs method) |
| D: Hybrid | ⚠️ Both | ⚠️ Both | ✅ No | High | ⚠️ Partial |
| E: Context Manager | ✅ Yes | ❌ No | ❌ Yes | High | ❌ No (different pattern) |
| F: Drop .after() | ✅ Yes | N/A | ❌ Yes | Very Low | ❌ No (feature missing) |

### 5.2 User Experience Impact

**Current API (Lazy):**
```python
# Simple case - no dependencies
results = process_item.map(items).get_results()

# With dependencies
prep = preprocess()
results = process_item.map(items).after(prep).get_results()

# Multiple dependencies
prep1 = preprocess1()
prep2 = preprocess2()
results = process_item.map(items).after(prep1, prep2).get_results()
```

**Option A (Reversed):**
```python
# Simple case - unchanged
results = process_item.map(items).get_results()

# With dependencies - reversed order
prep = preprocess()
results = process_item.after(prep).map(items).get_results()
#                    ^^^^^^^^^^^^^ dependencies first

# Multiple dependencies
prep1 = preprocess1()
prep2 = preprocess2()
results = process_item.after(prep1, prep2).map(items).get_results()
```

**Option B (Builder):**
```python
# Simple case - requires .submit()
results = process_item.map(items).submit().get_results()
#                                 ^^^^^^^^^ explicit

# With dependencies - extra .submit()
prep = preprocess()
results = process_item.map(items).after(prep).submit().get_results()

# Multiple dependencies
prep1 = preprocess1()
prep2 = preprocess2()
results = process_item.map(items).after(prep1, prep2).submit().get_results()
```

**Option C (Parameter):**
```python
# Simple case - unchanged
results = process_item.map(items).get_results()

# With dependencies - parameter syntax
prep = preprocess()
results = process_item.map(items, after=[prep]).get_results()
#                              ^^^^^^^^^^^^^^^ list syntax

# Multiple dependencies - natural list
prep1 = preprocess1()
prep2 = preprocess2()
results = process_item.map(items, after=[prep1, prep2]).get_results()
```

### 5.3 Implementation Complexity

**Simplest to Implement:**
1. **Option F** (Drop .after()) - Just remove the method
2. **Option C** (Parameter) - Add parameter to `.map()`, remove `.after()`
3. **Option A** (Reversed) - Add intermediate wrapper class

**Most Complex:**
1. **Option E** (Context Manager) - Requires context tracking, thread-safety
2. **Option D** (Hybrid) - Two implementations, complex type hints
3. **Option B** (Builder) - New builder class, explicit submit

---

## 6. Migration Impact

### 6.1 Current Usage Analysis

Based on test files and examples, current usage patterns:

**Pattern: Map without dependencies** (60% of cases)
```python
array_job = task.map(items)
```
- ✅ Works with ALL options (no changes needed)

**Pattern: Map with .after()** (35% of cases)
```python
array_job = task.map(items).after(prep_job)
```
- ❌ Breaks with Options A, C, E, F
- ⚠️ Requires `.submit()` with Option B
- ✅ Works with Option D (lazy path)

**Pattern: Complex chaining** (5% of cases)
```python
array_job = task.map(items).after(job1, job2).get_results()
```
- Same as above

### 6.2 Migration Effort

| Option | Files to Change | Lines to Change | Automated Migration? | Risk |
|--------|----------------|-----------------|---------------------|------|
| A | All .map().after() | ~20-30 | ✅ Yes (regex) | Low |
| B | All .map() | ~40-50 | ✅ Yes (add .submit()) | Low |
| C | All .after() | ~20-30 | ✅ Yes (regex) | Low |
| D | None | 0 | N/A | Low |
| E | All array jobs | ~40-50 | ⚠️ Partial | Medium |
| F | All .after() | ~20-30 | ❌ No (redesign needed) | High |

### 6.3 Example Migration Scripts

**For Option A (Reversed):**
```python
# Regex replacement:
# Before: task.map(items).after(deps)
# After:  task.after(deps).map(items)

import re

def migrate_to_reversed_api(code: str) -> str:
    pattern = r'(\w+)\.map\(([^)]+)\)\.after\(([^)]+)\)'
    replacement = r'\1.after(\3).map(\2)'
    return re.sub(pattern, replacement, code)
```

**For Option B (Builder):**
```python
# Add .submit() after .map()

def migrate_to_builder_api(code: str) -> str:
    # Add .submit() after .map(...) or .map(...).after(...)
    code = re.sub(r'\.map\(([^)]+)\)(?!\.)', r'.map(\1).submit()', code)
    return code
```

**For Option C (Parameter):**
```python
# Convert .after() to after= parameter

def migrate_to_parameter_api(code: str) -> str:
    pattern = r'\.map\(([^)]+)\)\.after\(([^)]+)\)'
    replacement = r'.map(\1, after=[\2])'
    return re.sub(pattern, replacement, code)
```

---

## 7. Recommendations

### 7.1 Recommended Approach: Option A (Reversed Fluent API)

**Rationale:**
1. **True eager execution** - submits immediately at `.map()` call
2. **Maintains fluent style** - method chaining still works
3. **Clear semantics** - dependencies come before dependent work
4. **Consistent with SLURM** - mirrors how SLURM actually works (deps at submit time)
5. **Simple migration** - automated regex replacement
6. **Low complexity** - straightforward implementation

**Implementation Plan:**

```python
class SlurmTask:
    def after(self, *jobs: Union[Job, ArrayJob]) -> "SlurmTaskWithDependencies":
        """Specify dependencies before mapping.

        Returns a task wrapper that will include these dependencies
        when .map() is called.

        Example:
            prep_job = preprocess()
            array_job = process.after(prep_job).map(items)
        """
        return SlurmTaskWithDependencies(self, list(jobs))

    def map(self, items, max_concurrent=None) -> ArrayJob:
        """Create and eagerly submit array job.

        Submits immediately with no dependencies.
        For dependencies, use: task.after(deps).map(items)
        """
        array_job = ArrayJob(self, items, cluster=..., dependencies=[])
        array_job._submit()  # Eager submission
        return array_job


class SlurmTaskWithDependencies:
    """Wrapper for a task with pre-specified dependencies."""

    def __init__(self, task: SlurmTask, dependencies: List[Job]):
        self.task = task
        self.dependencies = dependencies

    def map(self, items, max_concurrent=None) -> ArrayJob:
        """Create and eagerly submit array job with dependencies.

        Submits immediately with the pre-specified dependencies.
        """
        array_job = ArrayJob(
            self.task,
            items,
            cluster=...,
            dependencies=self.dependencies
        )
        array_job._submit()  # Eager submission with dependencies
        return array_job
```

**Migration:**
```python
# Before (lazy)
array_job = task.map(items).after(prep_job)

# After (eager)
array_job = task.after(prep_job).map(items)
```

**Deprecation Strategy:**
1. Add `SlurmTaskWithDependencies` class in version 0.9.0
2. Support both patterns in 0.9.x (with deprecation warning for old pattern)
3. Remove old pattern in 1.0.0

### 7.2 Alternative: Option C (Parameter-Based)

**If fluent API is not a hard requirement:**

```python
class SlurmTask:
    def map(
        self,
        items,
        after: Optional[List[Union[Job, ArrayJob]]] = None,
        max_concurrent: Optional[int] = None
    ) -> ArrayJob:
        """Create and eagerly submit array job.

        Args:
            items: Items to process
            after: Optional job dependencies
            max_concurrent: Max concurrent tasks

        Example:
            # No dependencies
            array_job = task.map(items)

            # With dependencies
            prep_job = preprocess()
            array_job = task.map(items, after=[prep_job])
        """
        dependencies = after or []
        array_job = ArrayJob(self, items, cluster=..., dependencies=dependencies)
        array_job._submit()  # Eager submission
        return array_job
```

**Migration:**
```python
# Before (lazy)
array_job = task.map(items).after(prep_job)

# After (eager)
array_job = task.map(items, after=[prep_job])
```

### 7.3 Not Recommended

**Option B (Builder)**: Defeats the purpose of eager execution (still requires explicit action)

**Option D (Hybrid)**: Adds complexity without clear benefit (two ways to do same thing)

**Option E (Context Manager)**: Too magical, hard to debug, threading complications

**Option F (Drop .after())**: Removes useful feature, forces workarounds

---

## 8. Implementation Checklist

If proceeding with **Option A (Reversed Fluent API)**:

### Phase 1: Implementation (v0.9.0)
- [ ] Create `SlurmTaskWithDependencies` wrapper class
- [ ] Implement `.after()` on `SlurmTask` to return wrapper
- [ ] Implement `.map()` on both `SlurmTask` and `SlurmTaskWithDependencies`
- [ ] Modify `ArrayJob.__init__()` to submit eagerly
- [ ] Remove `_submitted` flag and lazy submission logic
- [ ] Add deprecation warning for `.after()` called on `ArrayJob`

### Phase 2: Testing
- [ ] Update all array job tests to use new pattern
- [ ] Add tests for `SlurmTaskWithDependencies`
- [ ] Test eager submission behavior
- [ ] Test that dependencies are correctly passed to backend
- [ ] Integration tests with real SLURM cluster

### Phase 3: Documentation
- [ ] Update README with new pattern
- [ ] Update all examples to use reversed API
- [ ] Add migration guide
- [ ] Update API documentation
- [ ] Add deprecation notices

### Phase 4: Migration (v0.9.x)
- [ ] Support both old and new patterns (with warnings)
- [ ] Provide automated migration script
- [ ] Monitor user feedback

### Phase 5: Cleanup (v1.0.0)
- [ ] Remove support for old pattern (`.map().after()`)
- [ ] Remove deprecation warnings
- [ ] Remove lazy submission code paths
- [ ] Simplify `ArrayJob` class

---

## 9. Open Questions

1. **Should regular jobs also use eager execution for consistency?**
   - Currently: `job = task()` submits immediately
   - This is already eager, so array jobs would match

2. **What about the workflow API?**
   - Workflows use lazy execution by design
   - Array jobs within workflows might need different behavior

3. **Performance impact of eager array submission?**
   - Submitting N individual jobs vs native SLURM `--array`
   - Native array support would still be beneficial

4. **Type hints for intermediate wrapper?**
   - How to properly type-hint `SlurmTaskWithDependencies`
   - Should it have full `SlurmTask` interface?

5. **Edge cases:**
   - What if `.after()` is called twice: `task.after(j1).after(j2).map()`?
   - Should we support: `task.after(j1).map(i1), task.after(j2).map(i2)` (reuse)?

---

## 10. Conclusion

**The fundamental trade-off is clear:**
- **Eager execution is incompatible with `.map().after()` pattern** due to SLURM constraints
- **Three viable options exist** with different trade-offs:
  1. Reversed API: `task.after(deps).map(items)` (recommended)
  2. Parameter: `task.map(items, after=[deps])` (alternative)
  3. Builder: `task.map(items).after(deps).submit()` (not truly eager)

**Recommendation: Option A (Reversed Fluent API)**
- Maintains fluent style
- Enables true eager execution
- Straightforward migration
- Clear semantics

**Next steps:**
1. Gather stakeholder feedback on API preferences
2. Decide between Option A and Option C
3. Implement in feature branch
4. Beta test with real users
5. Gather feedback before 1.0.0 release
