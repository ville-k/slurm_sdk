from slurm import runtime
from slurm.decorators import task
from slurm.runtime import (
    JobContext,
    _bind_job_context,
    build_job_context,
    _function_wants_job_context,
)


def test_job_context_from_env(monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", "42")
    monkeypatch.setenv("SLURM_STEP_ID", "1")
    monkeypatch.setenv("SLURM_NODELIST", "node[01-02]")
    monkeypatch.setenv("SLURM_PROCID", "3")
    monkeypatch.setenv("SLURM_LOCALID", "1")
    monkeypatch.setenv("SLURM_NODEID", "1")
    monkeypatch.setenv("SLURM_NTASKS", "8")
    monkeypatch.setenv("SLURM_NNODES", "2")
    monkeypatch.setenv("SLURM_NTASKS_PER_NODE", "4")
    monkeypatch.setenv("MASTER_PORT", "12345")

    context = build_job_context()

    assert isinstance(context, JobContext)
    assert context.job_id == "42"
    assert context.world_size == 8
    assert context.node_rank == 1
    assert context.hostnames == ("node01", "node02")
    assert context.master_port == 12345
    torch_env = context.torch_distributed_env()
    assert torch_env["MASTER_ADDR"] == "node01"
    assert torch_env["LOCAL_RANK"] == "1"
    assert torch_env["WORLD_SIZE"] == "8"


def test_current_job_context_caches(monkeypatch):
    monkeypatch.delenv("SLURM_NODELIST", raising=False)
    monkeypatch.setenv("SLURM_JOB_ID", "99")
    runtime._CONTEXT = None  # reset cache for test
    first = runtime.current_job_context()
    second = runtime.current_job_context()
    assert first is second


def test_bind_job_context_inserts():
    def sample(value: int, job: JobContext) -> int:
        return value + (1 if job.world_size else 0)

    args = (10,)
    kwargs: dict = {}
    context = build_job_context({"SLURM_NTASKS": "2"})
    new_args, new_kwargs, injected = _bind_job_context(sample, args, kwargs, context)

    assert injected is True
    assert new_args[0] == 10
    assert new_args[1] is context
    assert new_kwargs == {}
    assert _function_wants_job_context(sample) is True

    def sample_kw(*, job: JobContext, value: int = 0) -> int:
        return value

    args = ()
    kwargs = {"value": 7}
    new_args, new_kwargs, injected = _bind_job_context(sample_kw, args, kwargs, context)
    assert injected is True
    assert new_args == ()
    assert new_kwargs["job"] is context


def test_bind_job_context_handles_slurm_task_object():
    @task
    def wrapped(value: int, job: JobContext) -> int:
        return value + (job.world_size or 0)

    context = build_job_context({"SLURM_NTASKS": "3"})
    new_args, new_kwargs, injected = _bind_job_context(wrapped, (5,), {}, context)

    assert injected is True
    assert new_args[0] == 5
    assert new_args[1] is context
    assert _function_wants_job_context(wrapped) is True
