"""Example showing how to chain jobs together by passing results.

This example shows:
- Submitting multiple jobs sequentially
- Passing the result of one job as input to the next
- Using cluster defaults for consistent configuration
"""

import logging
import argparse
import socket
import time

from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


@task(
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
    parser = argparse.ArgumentParser(
        description="Chain jobs together on a SLURM cluster"
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    # Add example-specific arguments
    parser.add_argument(
        "--banner-timeout",
        type=int,
        default=30,
        help="Timeout for waiting for SSH banner (seconds)",
    )
    parser.add_argument("--loglevel", type=str, default="INFO", help="Logging level")

    args = parser.parse_args()

    configure_logging(level=getattr(logging, args.loglevel.upper(), logging.INFO))

    # Create cluster from args
    cluster = Cluster.from_args(
        args,
        banner_timeout=args.banner_timeout,
    )

    # Submit the first job
    hello_job = hello_world.submit(cluster)()

    # Wait for the job to complete
    hello_job.wait()

    # Get the result data directly from the first job
    print("Retrieving result from hello_job...")
    hello_result_content = hello_job.get_result()
    print(f"Retrieved result content: '{hello_result_content}'")

    # Submit the second job with the result data
    print("Submitting process_results job with result content...")
    process_job = process_results.submit(cluster)(hello_result_content)

    # Wait for the second job to complete
    process_job.wait()

    # Get the final result
    final_result = process_job.get_result()
    print(f"Final result: {final_result}")


if __name__ == "__main__":
    main()
