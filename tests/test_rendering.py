from slurm.rendering import render_job_script
from slurm.callbacks.callbacks import BaseCallback
from slurm.packaging.base import PackagingStrategy


class DummyStrategy(PackagingStrategy):
    def prepare(self, task, cluster):
        return {"status": "ok"}

    def generate_setup_commands(self, task, job_id=None, job_dir=None):
        return ["echo setup"]

    def generate_cleanup_commands(self, task, job_id=None, job_dir=None):
        return ["echo cleanup"]


def sample_func(x, y=1):
    return x + y


def test_task_decorator_without_arguments():
    from slurm.decorators import task

    @task
    def decorated() -> None:
        return None

    assert decorated.sbatch_options["job_name"] == "decorated"


def test_render_job_script_basic_includes_sbatch_and_runner(tmp_path):
    script = render_job_script(
        task_func=sample_func,
        task_args=(1,),
        task_kwargs={"y": 2},
        task_definition={
            "job_name": "unit-test",
            "time": "00:01:00",
            "mem": "1G",
            "cpus_per_task": 1,
            "nodes": 1,
            "ntasks": 1,
            "account": "acc",
            "partition": "part",
            "exclusive": None,
        },
        sbatch_overrides={},
        packaging_strategy=DummyStrategy({}),
        target_job_dir=str(tmp_path),
        pre_submission_id="abc123",
        callbacks=[BaseCallback()],
    )

    assert "#SBATCH --job-name=unit-test" in script
    assert "#SBATCH --time=00:01:00" in script
    assert "#SBATCH --mem=1G" in script
    assert "#SBATCH --cpus-per-task=1" in script
    assert "#SBATCH --nodes=1" in script
    assert "#SBATCH --ntasks=1" in script
    assert "#SBATCH --account=acc" in script
    assert "#SBATCH --partition=part" in script
    assert "#SBATCH --exclusive" in script.splitlines()
    assert '"${PY_EXEC[@]:-python}" -m slurm.runner' in script
    assert f'--job-dir "{str(tmp_path)}"' in script
    assert f'--stdout-path "{str(tmp_path)}/slurm_abc123.out"' in script
    assert f'--stderr-path "{str(tmp_path)}/slurm_abc123.err"' in script
    assert '--pre-submission-id "abc123"' in script
    assert "JOB_DIR=" in script
    assert "echo setup" in script
    assert "echo cleanup" in script


def test_render_job_script_gpu_and_per_node_options(tmp_path):
    script = render_job_script(
        task_func=sample_func,
        task_args=(1,),
        task_kwargs={"y": 2},
        task_definition={
            "job_name": "gpu-test",
            "nodes": 2,
            "ntasks": 2,
            "ntasks_per_node": 8,
            "cpus_per_task": 4,
            "gpus_per_node": 8,
            "gpus": 8,
            "gpus_per_task": 1,
            "gres": "gpu:8",
        },
        sbatch_overrides={},
        packaging_strategy=DummyStrategy({}),
        target_job_dir=str(tmp_path),
        pre_submission_id="gid123",
        callbacks=[BaseCallback()],
    )

    # Per-node and GPU options
    assert "#SBATCH --ntasks-per-node=8" in script
    assert "#SBATCH --gpus-per-node=8" in script
    # Generic GPUs and per-task
    assert "#SBATCH --gpus=8" in script
    assert "#SBATCH --gpus-per-task=1" in script
    # GRES still included
    assert "#SBATCH --gres=gpu:8" in script


def test_render_job_script_handles_arbitrary_directives(tmp_path):
    script = render_job_script(
        task_func=sample_func,
        task_args=(),
        task_kwargs={},
        task_definition={
            "time": "00:05:00",
            "mail_type": "FAIL",
            "array": "0-3",
        },
        sbatch_overrides={"reservation": "debug"},
        packaging_strategy=DummyStrategy({}),
        target_job_dir=str(tmp_path),
        pre_submission_id="arr123",
        callbacks=[BaseCallback()],
    )

    assert "#SBATCH --job-name=sample_func" in script
    assert "#SBATCH --mail-type=FAIL" in script
    assert "#SBATCH --array=0-3" in script
    assert "#SBATCH --reservation=debug" in script


def test_render_container_job_with_uv_run_python(tmp_path):
    """Test that container packaging with 'uv run python' renders correctly."""
    from slurm.packaging.container import ContainerPackagingStrategy

    container_strategy = ContainerPackagingStrategy(
        {
            "image": "my-uv-image:latest",
            "python_executable": "uv run python",
        }
    )
    # Simulate prepare being called
    container_strategy._image_reference = "my-uv-image:latest"

    script = render_job_script(
        task_func=sample_func,
        task_args=(1,),
        task_kwargs={"y": 2},
        task_definition={
            "job_name": "uv-container-test",
            "time": "00:10:00",
        },
        sbatch_overrides={},
        packaging_strategy=container_strategy,
        target_job_dir=str(tmp_path),
        pre_submission_id="uvtest123",
        callbacks=[BaseCallback()],
    )

    # Verify PY_EXEC is set as a bash array in setup commands
    assert "PY_EXEC=(uv run python)" in script

    # Verify srun uses the array variable reference directly
    assert "srun" in script
    assert "--container-image=my-uv-image:latest" in script
    assert '"${PY_EXEC[@]:-python}" -m slurm.runner' in script
