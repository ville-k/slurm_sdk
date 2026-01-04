import argparse
import logging

from slurm import Cluster, Job, task
from slurm.callbacks.callbacks import RichLoggerCallback


@task(
    nodes=1,
    ntasks_per_node=1,
    time="00:03:00",
    mem="512M",
)
def hello_torch() -> str:
    import os
    import socket
    import torch

    hostname = socket.gethostname()
    print(f"Hostname: {hostname}")

    # List CPU information
    cpu_count = os.cpu_count() or 1
    print(f"CPU cores available: {cpu_count}")

    # List GPU information (if available)
    cuda_available = torch.cuda.is_available()
    cuda_devices = torch.cuda.device_count() if cuda_available else 0

    print(f"CUDA available: {cuda_available}")
    print(f"CUDA devices: {cuda_devices}")

    if cuda_available and cuda_devices > 0:
        for i in range(cuda_devices):
            print(f"  GPU {i}: {torch.cuda.get_device_name(i)}")
        cuda_version = torch.version.cuda or "N/A"
        return f"PyTorch {torch.__version__} with CUDA {cuda_version}, {cuda_devices} GPU(s), {cpu_count} CPU(s)"
    else:
        return f"PyTorch {torch.__version__} (CPU only), {cpu_count} CPU(s)"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Submit a PyTorch task using a container",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    Cluster.add_argparse_args(parser)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

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


if __name__ == "__main__":
    main()
