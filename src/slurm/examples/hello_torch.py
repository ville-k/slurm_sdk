import argparse
import logging
from slurm import Cluster, Job, task
from slurm.callbacks.callbacks import RichLoggerCallback


@task(
    gpus_per_node=8,
    ntasks_per_node=1,
    exclusive=None,
    packaging="container",
    packaging_platform="linux/amd64",
    packaging_push=True,
    packaging_registry="nvcr.io/nv-maglev/",
    packaging_dockerfile="src/slurm/examples/hello_torch.Dockerfile",
    packaging_context=".",
)
def hello_torch() -> str:
    import torch

    return f"Pytorch {torch.__version__} is installed with CUDA {torch.version.cuda} devices: {torch.cuda.device_count()}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Submit a PyTorch task using a container",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    Cluster.add_argparse_args(parser)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    with Cluster.from_args(args, callbacks=[RichLoggerCallback()]):
        job: Job[str] = hello_torch()
        success = job.wait()
        if success:
            result: str = job.get_result()
            print(f"Result: {result}")
        else:
            print("Job failed!")
            print("Job std out:")
            print(job.get_stdout())
            print("Job std err:")
            print(job.get_stderr())
