from typing import Any, Dict, List

from slurm.cluster import Cluster
from slurm.job import Job


class FakeBackend:
    def __init__(self):
        self._queue = [
            {
                "JOBID": "101",
                "NAME": "jobA",
                "STATE": "COMPLETED",
                "USER": "u",
                "TIME": "0",
                "TIME_LIMIT": "0",
            },
            {
                "JOBID": "102",
                "NAME": "jobB",
                "STATE": "RUNNING",
                "USER": "u",
                "TIME": "0",
                "TIME_LIMIT": "0",
            },
        ]
        self._status: Dict[str, Dict[str, Any]] = {
            "101": {"JobState": "COMPLETED", "ExitCode": "0:0"},
            "102": {"JobState": "RUNNING"},
        }

    def get_queue(self) -> List[Dict[str, Any]]:
        return self._queue

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        return self._status.get(job_id, {"JobState": "UNKNOWN"})

    def get_cluster_info(self) -> Dict[str, Any]:
        return {"partitions": []}

    def submit_job(self, *args, **kwargs) -> str:
        return "999"

    def cancel_job(self, job_id: str) -> bool:
        self._status[job_id] = {"JobState": "CANCELLED"}
        return True


def test_cluster_get_jobs_uses_queue(monkeypatch):
    # Create a Cluster-like instance without invoking __init__ (avoids backend factory)
    c = object.__new__(Cluster)
    c.backend = FakeBackend()

    jobs = c.get_jobs()
    assert isinstance(jobs, list)
    assert all(isinstance(j, Job) for j in jobs)
    assert {j.id for j in jobs} == {"101", "102"}
