# Python SLURM SDK

A developer-friendly SDK to define, submit, and manage SLURM jobs in Python.

## Quick start

```python
from slurm import Cluster, task
from slurm.logging import configure_logging

configure_logging()  # pleasant defaults; use DEBUG for more detail

@task(job_name="hello", time="00:01:00", mem="1G", ntasks=1, nodes=1)
def hello(name: str) -> str:
    return f"Hello {name}!"

if __name__ == "__main__":
    # Local test backend for offline dev
    cluster = Cluster(backend_type="local")
    job = hello.submit(cluster=cluster, packaging={"type": "none"})("world")
    job.wait()
    print(job.get_result())
```

SSH backend example:

```python
cluster = Cluster(backend_type="ssh", hostname="login.myhpc.example", username="me")
job = hello.submit(cluster=cluster)("cluster")
job.wait()
print(job.get_result())
```

## Testing

- Unit tests (offline):
  - Local backend executes tasks without external services.
  - Run: `uv run pytest`.

- Optional SSH integration (not included by default):
  - Configure env vars/CLI for host/user; write tests with markers in your project.

## Docs and references

- SLURM REST API: https://slurm.schedmd.com/SC24/REST-API.pdf
- SLURM Containers: https://slurm.schedmd.com/SC24/Containers.pdf


## Running on an ARM Mac (Apple Silicon)

You can build and publish x86_64 (``linux/amd64```) images from an ARM Mac by
enabling QEMU emulation inside your container runtime. The example below shows
the required one-time setup for Podman; Docker Desktop users can follow a
similar flow using `docker buildx` (no additional configuration usually needed).

1. Install QEMU locally. This provides the binary emulators that Podman will
   load into its virtual machine:

   ```sh
   brew install qemu
   ```

2. Make sure the Podman machine is created and running. If you have not
   initialised it yet:

   ```sh
   podman machine init --now
   ```

3. Register the QEMU static binaries inside the Podman machine so it can run
   amd64 containers. This step reboots the VM and only needs to be performed
   once (repeat if you recreate the machine):

   ```sh
   podman machine ssh sudo rpm-ostree install qemu-user-static
   podman machine ssh sudo systemctl reboot
   ```

4. After the machine restarts, confirm cross-building works:

   ```sh
   podman build --platform linux/amd64 -t test-amd64 .
   podman run --rm --platform linux/amd64 test-amd64 uname -m
   ```

   The final command should print `x86_64` even though you are on an ARM host.

With QEMU registered you can now build the example container in this project
using the provided Slurmfile (`platform = "linux/amd64"`) and push it to your
registry before submitting jobs to an x86 cluster.
