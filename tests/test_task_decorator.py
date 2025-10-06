from slurm.decorators import task
from slurm.runtime import JobContext, _function_wants_job_context
from slurm.task import SlurmTask


def test_task_decorator_sets_defaults_and_job_name():
    @task(job_name="unit-job", time="00:01:00", mem="2G")
    def f(a, b=1):
        return a + b

    assert isinstance(f, SlurmTask)
    assert f.sbatch_options["job_name"] == "unit-job"
    assert f.sbatch_options["time"] == "00:01:00"
    assert f.sbatch_options["mem"] == "2G"


def test_task_decorator_detects_job_context_signature():
    @task(time="00:01:00")
    def g(value: int, *, job: JobContext) -> int:
        return value

    assert isinstance(g, SlurmTask)
    assert _function_wants_job_context(g.func) is True
