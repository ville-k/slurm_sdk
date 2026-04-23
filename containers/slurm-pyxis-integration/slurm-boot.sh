#!/bin/bash
# Role-based Slurm boot configuration.
#
# SLURM_ROLE=worker    → mask slurmctld.service (only slurmd runs).
# SLURM_ROLE=controller or unset → no action (run slurmctld + slurmd).
#
# Reads SLURM_ROLE from PID 1's environment because systemd services
# do not inherit the container runtime's environment by default.
set -euo pipefail

ROLE=$(tr '\0' '\n' < /proc/1/environ | grep '^SLURM_ROLE=' | cut -d= -f2- || true)

if [[ "${ROLE:-controller}" == "worker" ]]; then
    echo "slurm-boot: SLURM_ROLE=worker — masking slurmctld.service"
    systemctl mask slurmctld.service
fi
