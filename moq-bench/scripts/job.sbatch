#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Four identical nodes: relay, publisher, two subscriber nodes.
#   sbatch moq-bench/scripts/job.sbatch slow
# Adjust partition/QOS for your ICE allocation. --exclusive keeps other jobs
# off the nodes; drop it if your account cannot request it.
#SBATCH --job-name=moq-bench
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --exclusive
#SBATCH --output=moq-bench-%j.out
set -euo pipefail
cd "${SLURM_SUBMIT_DIR:-.}"
scontrol show hostnames "$SLURM_JOB_NODELIST"
exec moq-bench/scripts/sweep.sh "${1:-calibrate}" "${2:-baseline}"
