import logging
import argparse
import os
import socket
import time

from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


@task(
    name="hello_world",
    partition="cpu",
    time="00:05:00",
    mem="1G",
    cpus_per_task=1,
)
def hello_world():
    """
    A simple hello world task.

    Returns:
        A greeting message
    """
    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello from {hostname} at {current_time}!"
    print(message)

    return message


@task(
    name="process_results",
    partition="cpu",
    time="00:05:00",
    mem="1G",
    cpus_per_task=1,
)
def process_results(result_content: str):
    """
    Process the results (content) from a previous job.

    Args:
        result_content: The string content returned by the previous job.

    Returns:
        The processed results
    """
    print(f"Processing result content: '{result_content}'...")
    words = result_content.split()
    word_count = len(words)

    print(f"Word count: {word_count}")

    return {"word_count": word_count, "content": result_content}


def main():
    """
    Main entry point for the example script.
    """
    parser = argparse.ArgumentParser(description="Submit jobs to a SLURM cluster")
    parser.add_argument("--hostname", required=True, help="Hostname for SSH backend")
    parser.add_argument(
        "--username",
        default=os.environ.get("USER"),
        help="Username for SSH backend. Defaults to current user's username.",
    )
    parser.add_argument("--account", help="SLURM account")
    parser.add_argument("--partition", help="SLURM partition")
    parser.add_argument(
        "--banner-timeout",
        type=int,
        default=30,
        help="Timeout for waiting for SSH banner (seconds)",
    )
    parser.add_argument("--loglevel", type=str, default="INFO", help="Logging level")
    args = parser.parse_args()

    configure_logging(level=getattr(logging, args.loglevel.upper(), logging.INFO))
    backend_kwargs = {}
    backend_kwargs["hostname"] = args.hostname
    backend_kwargs["username"] = args.username
    backend_kwargs["banner_timeout"] = args.banner_timeout
    account = args.account
    partition = args.partition

    _ = Cluster(
        backend_type="ssh",
        **backend_kwargs,
    )

    # Submit the first job
    hello_job = hello_world.submit(
        account=account,
        partition=partition,
        packaging={
            "type": "wheel",
            "python_version": "3.9",
        },
    )()

    # Wait for the job to complete
    hello_job.wait()

    # Get the result data directly from the first job
    print("Retrieving result from hello_job...")
    hello_result_content = hello_job.get_result()
    print(f"Retrieved result content: '{hello_result_content}'")

    # Submit the second job with the result data
    print("Submitting process_results job with result content...")
    process_job = process_results.submit(
        account=account,
        partition=partition,
        packaging={
            "type": "wheel",
            "python_version": "3.9",
        },
    )(hello_result_content)

    # Wait for the second job to complete
    process_job.wait()

    # Get the final result
    final_result = process_job.get_result()
    print(f"Final result: {final_result}")


if __name__ == "__main__":
    main()
