import os
import tempfile
import time
import subprocess
from typing import Any, Dict, List

from slurm.errors import BackendCommandError


class LocalBackend:
    """A minimal local backend suitable for tests."""

    def __init__(self, job_base_dir: str = None):
        self.job_base_dir = job_base_dir or os.path.join(
            tempfile.gettempdir(), "slurm_jobs"
        )
        os.makedirs(self.job_base_dir, exist_ok=True)
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def submit_job(
        self,
        script: str,
        target_job_dir: str,
        pre_submission_id: str,
        account: str = None,
        partition: str = None,
        array_spec: str = None,
    ) -> str:
        """Submit a job. Note: array_spec is accepted but not supported by this simple backend."""
        os.makedirs(target_job_dir, exist_ok=True)
        # Use the same naming convention as the real backends
        script_path = os.path.join(
            target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"
        )
        with open(script_path, "w", newline="\n") as f:
            f.write(script)
        os.chmod(script_path, 0o755)

        job_id = str(int(time.time() * 1000))

        # For array jobs, don't execute the script (it expects SLURM env vars)
        # Just return the array job ID format for testing
        if array_spec:
            self._jobs[job_id] = {
                "dir": target_job_dir,
                "submitted": time.time(),
                "JobState": "COMPLETED",
                "ExitCode": "0:0",
                "Stdout": "",
                "Stderr": "",
            }
            return f"{job_id}_[{array_spec}]"

        # For regular jobs, execute the script
        env = os.environ.copy()
        proc = subprocess.Popen(
            [script_path],
            cwd=target_job_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._jobs[job_id] = {
            "process": proc,
            "dir": target_job_dir,
            "submitted": time.time(),
        }
        stdout, stderr = proc.communicate()
        exit_code = proc.returncode
        self._jobs[job_id].update(
            {
                "JobState": "COMPLETED" if exit_code == 0 else "FAILED",
                "ExitCode": f"{exit_code}:0",
                "Stdout": stdout.decode(errors="ignore"),
                "Stderr": stderr.decode(errors="ignore"),
            }
        )

        return job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        job = self._jobs.get(job_id)
        if not job:
            raise BackendCommandError(f"Job not found: {job_id}")
        return {k: job[k] for k in ("JobState", "ExitCode") if k in job}

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job:
            return False
        proc = job.get("process")
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
            job["JobState"] = "CANCELLED"
            job["ExitCode"] = "1:0"
        return True

    def get_queue(self) -> List[Dict[str, Any]]:
        queue = []
        for job_id, meta in self._jobs.items():
            queue.append(
                {
                    "JOBID": job_id,
                    "NAME": os.path.basename(meta.get("dir", "")) or "job",
                    "STATE": meta.get("JobState", "UNKNOWN"),
                    "USER": os.environ.get("USER", "local"),
                    "TIME": "00:00:00",
                    "TIME_LIMIT": "00:00:00",
                }
            )
        return queue

    def get_cluster_info(self) -> Dict[str, Any]:
        return {
            "partitions": [
                {
                    "PARTITION": "local",
                    "AVAIL": "up",
                    "TIMELIMIT": "infinite",
                    "NODES": "1",
                    "STATE": "up",
                }
            ]
        }

    def is_remote(self) -> bool:
        """Return False since this is a local backend."""
        return False
