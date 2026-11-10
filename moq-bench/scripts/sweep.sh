#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Baseline sweeps from the project plan. Each configuration runs REPS times;
# summarize.py appends one row per run to $RESULTS/summary.csv.
#
# usage: sweep.sh <bandwidth|slow|heavy|calibrate|all> [tag]
#   tag (default "baseline") prefixes run ids, e.g. "baseline" vs "drr".
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
scenario=${1:?usage: sweep.sh <bandwidth|slow|heavy|calibrate|all> [tag]}
tag=${2:-baseline}
REPS=${REPS:-3}
export DURATION=${DURATION:-60}

run() { # run <id> VAR=value...
	local id=$1
	shift
	for rep in $(seq 1 "$REPS"); do
		env "$@" RUN_ID="$tag-$id-r$rep" "$here/run-one.sh" || echo "FAILED: $tag-$id-r$rep" >&2
		sleep 5 # let sockets drain between runs
	done
}

bandwidth() {
	# Shared send cap below aggregate demand (subs x 4 Mb/s).
	for subs in 32 64 128 256; do
		run "bw-s$subs-nocap" SUBS=$subs
		for cap in 100 250 500; do
			run "bw-s$subs-cap$cap" SUBS=$subs SEND_CAP_MBPS=$cap
		done
	done
}

slow() {
	# Slow receivers at fixed fan-out, no send cap.
	local subs=${SLOW_SUBS:-128}
	run "slow-s$subs-f0" SUBS=$subs
	for frac in 0.1 0.25 0.5; do
		for mode in rate:0.5 rate:1 pause:2000:1000 stall:10000 stall:10000:2000 stall:10000:5000; do
			run "slow-s$subs-f$frac-${mode//:/_}" SUBS=$subs SLOW_FRAC=$frac SLOW_MODE=$mode
		done
	done
}

heavy() {
	# Multi-track subscribers competing under a send cap.
	for frac in 0.1 0.25; do
		for cap in 250 500; do
			run "heavy-s128-f$frac-t8-cap$cap" SUBS=128 HEAVY_FRAC=$frac HEAVY_TRACKS=8 SEND_CAP_MBPS=$cap
		done
	done
}

calibrate() {
	# Generator headroom check: drive 1.5x the largest planned load and confirm
	# sub*_cpu_cores in summary.csv stays well below the node's core count and
	# healthy delivery stays at 1.0 with no cap.
	REPS=1 run "calib-s384" SUBS=384
	REPS=1 run "calib-s768" SUBS=768
}

case "$scenario" in
bandwidth) bandwidth ;;
slow) slow ;;
heavy) heavy ;;
calibrate) calibrate ;;
all) calibrate; bandwidth; slow; heavy ;;
*) echo "unknown scenario $scenario" >&2; exit 2 ;;
esac
