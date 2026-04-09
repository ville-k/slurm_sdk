"""Tests for shared Slurm command output parsers."""

import pytest

from slurm.api._parsing import (
    parse_scontrol_status,
    parse_sacct_accounting,
    parse_sacct_account_jobs,
    parse_squeue_output,
    parse_sinfo_output,
)


class TestParseScontrolStatus:
    def test_basic_key_value(self):
        stdout = "JobId=12345 JobName=test JobState=RUNNING\nNumNodes=1 Partition=gpu"
        result = parse_scontrol_status(stdout)
        assert result["JobId"] == "12345"
        assert result["JobState"] == "RUNNING"
        assert result["Partition"] == "gpu"

    def test_value_with_equals(self):
        """Values containing '=' are split correctly (only first '=' is the delimiter)."""
        stdout = "JobId=12345 WorkDir=/home/user ExitCode=0:0"
        result = parse_scontrol_status(stdout)
        assert result["JobId"] == "12345"
        assert result["WorkDir"] == "/home/user"

    def test_empty_input(self):
        assert parse_scontrol_status("") == {}
        assert parse_scontrol_status("  \n  ") == {}


class TestParseSacctAccounting:
    def test_basic_accounting(self):
        stdout = "12345|COMPLETED|0:0|2026-04-01T10:00:00|2026-04-01T11:00:00|01:00:00"
        result = parse_sacct_accounting(stdout, "12345")
        assert result["JobID"] == "12345"
        assert result["JobState"] == "COMPLETED"
        assert result["ExitCode"] == "0:0"
        assert result["Elapsed"] == "01:00:00"

    def test_empty_output_raises(self):
        with pytest.raises(ValueError, match="No accounting data"):
            parse_sacct_accounting("", "999")

    def test_short_output_raises(self):
        with pytest.raises(ValueError, match="Unexpected sacct output"):
            parse_sacct_accounting("12345|COMPLETED", "12345")

    def test_multiple_lines_uses_first(self):
        stdout = (
            "12345|COMPLETED|0:0|start|end|elapsed\n12345.batch|COMPLETED|0:0|s|e|el"
        )
        result = parse_sacct_accounting(stdout, "12345")
        assert result["JobID"] == "12345"


class TestParseSacctAccountJobs:
    SAMPLE_LINE = "100|train|user1|acct1|COMPLETED|0:0|gres/gpu=4|2|2026-04-01|2026-04-01|01:00:00|gpu"

    def test_basic_parsing(self):
        jobs = parse_sacct_account_jobs(self.SAMPLE_LINE)
        assert len(jobs) == 1
        assert jobs[0]["JobID"] == "100"
        assert jobs[0]["JobName"] == "train"
        assert jobs[0]["State"] == "COMPLETED"
        assert jobs[0]["Partition"] == "gpu"

    def test_skips_job_steps(self):
        stdout = f"{self.SAMPLE_LINE}\n100.batch|train|user1|acct1|COMPLETED|0:0|gres/gpu=4|2|s|e|el|gpu"
        jobs = parse_sacct_account_jobs(stdout)
        assert len(jobs) == 1

    def test_skips_short_lines(self):
        stdout = f"{self.SAMPLE_LINE}\nshort|line"
        jobs = parse_sacct_account_jobs(stdout)
        assert len(jobs) == 1

    def test_empty_input(self):
        assert parse_sacct_account_jobs("") == []

    def test_empty_alloc_tres(self):
        line = "100|train|user1|acct1|COMPLETED|0:0||2|s|e|el|gpu"
        jobs = parse_sacct_account_jobs(line)
        assert jobs[0]["AllocTRES"] == ""
        assert jobs[0]["AllocGRES"] == ""


class TestParseSqueueOutput:
    def test_basic_queue(self):
        stdout = (
            "12345|train|RUNNING|user1|2026-04-01|01:00:00|02:00:00|gpu|acct1|1|None"
        )
        jobs = parse_squeue_output(stdout)
        assert len(jobs) == 1
        assert jobs[0]["JOBID"] == "12345"
        assert jobs[0]["STATE"] == "RUNNING"
        assert jobs[0]["PARTITION"] == "gpu"

    def test_multiple_jobs(self):
        stdout = "100|a|RUNNING|u|s|t|tl|p|a|1|r\n101|b|PENDING|u|s|t|tl|p|a|1|r\n"
        jobs = parse_squeue_output(stdout)
        assert len(jobs) == 2

    def test_empty_input(self):
        assert parse_squeue_output("") == []

    def test_short_lines_skipped(self):
        stdout = "12345|train|RUNNING|user1|2026-04-01|01:00:00|02:00:00|gpu|acct1|1|None\nshort"
        jobs = parse_squeue_output(stdout)
        assert len(jobs) == 1


class TestParseSinfoOutput:
    def test_basic_partitions(self):
        stdout = "gpu|up|2-00:00:00|4|idle"
        partitions = parse_sinfo_output(stdout)
        assert len(partitions) == 1
        assert partitions[0]["PARTITION"] == "gpu"
        assert partitions[0]["AVAIL"] == "up"
        assert partitions[0]["NODES"] == "4"

    def test_multiple_partitions(self):
        stdout = "gpu|up|2-00:00:00|4|idle\ncpu|up|7-00:00:00|20|mixed"
        partitions = parse_sinfo_output(stdout)
        assert len(partitions) == 2

    def test_empty_input(self):
        assert parse_sinfo_output("") == []
