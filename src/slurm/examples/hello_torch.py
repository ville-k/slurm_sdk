import argparse
import logging

from slurm import Cluster, Job, task
from slurm.callbacks.callbacks import RichLoggerCallback


@task(
    nodes=1,
    gpus_per_node=1,
    ntasks_per_node=1,
    exclusive=None,
)
def hello_torch() -> str:
    import socket
    import torch

    hostname = socket.gethostname()
    print(f"Hostname: {hostname}")

    cuda_devices = torch.cuda.device_count()

    print(f"CUDA devices: {cuda_devices}")
    for i in range(cuda_devices):
        print(f"CUDA device {i}: {torch.cuda.get_device_name(i)}")

    return f"Pytorch {torch.__version__} is installed with CUDA {torch.version.cuda} devices: {torch.cuda.device_count()}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Submit a PyTorch task using a container",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    Cluster.add_argparse_args(parser)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    # Create cluster with default container packaging
    cluster = Cluster.from_args(
        args,
        callbacks=[RichLoggerCallback()],
        default_packaging="container",
        default_packaging_dockerfile="src/slurm/examples/hello_torch.Dockerfile",
    )

    with cluster:
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
