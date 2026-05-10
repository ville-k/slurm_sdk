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

# Reconcile /home/slurm ownership when the directory comes from a
# freshly-created named volume. The container runtime creates the
# volume's mount-point as root, overlaying the image's slurm-owned
# /home/slurm dir. Subdirectories written by the runtime's "copy on
# first attach" inherit slurm:slurm correctly (the image had them so),
# but the mount-point itself stays root:root. That makes mkdir of
# siblings (e.g. /home/slurm/test_mount_data in
# tests/integration/test_container_packaging_advanced.py) fail with
# EACCES for the slurm user. Chown idempotently — no-op once the dir
# is already slurm-owned, including across container restarts.
if [[ -d /home/slurm ]] && [[ "$(stat -c %U /home/slurm)" != "slurm" ]]; then
    echo "slurm-boot: chown /home/slurm to slurm:slurm (named volume init)"
    chown slurm:slurm /home/slurm
fi
