"""High-availability training + evaluation example.

This example demonstrates a resilient, production-oriented workflow pattern:

- A scheduler-level supervisor workflow that uses a stable `run_dir` for all state.
- Idempotent, per-attempt artifact directories for train/eval tasks.
- Retry and resumption based on on-disk `result.json` records.
- Optional in-job resiliency hooks (NVRx-style) within train/eval tasks.
"""
