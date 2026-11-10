#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Run one experiment configuration and write results to $RESULTS/$RUN_ID.
#
# Inside a 4-node Slurm allocation, node 1 runs the relay, node 2 the
# publisher, and nodes 3-4 split the subscribers. Outside Slurm (LOCAL=1, or
# no SLURM_JOB_NODELIST) every role runs on this machine, for debugging.
#
# Configure with environment variables; defaults below. Example:
#   SUBS=128 SLOW_FRAC=0.25 SLOW_MODE=rate:1 RUN_ID=slow-128-r1 ./run-one.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)

export BIN=${BIN:-$repo/target/release}
export RESULTS=${RESULTS:-$repo/bench-results}
export CERT_DIR=${CERT_DIR:-$RESULTS/cert}
export RUN_ID=${RUN_ID:-run-$(date +%Y%m%d-%H%M%S)}
export OUT=$RESULTS/$RUN_ID
export PORT=${PORT:-4443}

# Workload
export SUBS=${SUBS:-32}
export DURATION=${DURATION:-60}       # subscriber run time, seconds
export BITRATE=${BITRATE:-4}          # Mb/s per track (synthetic)
export FPS=${FPS:-30}
export GOP_SECS=${GOP_SECS:-1}
export TRACE=${TRACE:-}               # ffprobe trace for media replay
export TRACKS=${TRACKS:-1}
export HEAVY_FRAC=${HEAVY_FRAC:-0}
export HEAVY_TRACKS=${HEAVY_TRACKS:-8}
export SLOW_FRAC=${SLOW_FRAC:-0}
export SLOW_MODE=${SLOW_MODE:-normal}
export STREAM_WIN=${STREAM_WIN:-65536}   # subscriber QUIC windows, so
export CONN_WIN=${CONN_WIN:-262144}      # backpressure reaches the relay
export RESUB_MS=${RESUB_MS:-}
export OBJECT_LOG=${OBJECT_LOG:-1}
export RAMP_MS=${RAMP_MS:-20}
export STATS_MS=${STATS_MS:-1000}

# Relay
export SEND_CAP_MBPS=${SEND_CAP_MBPS:-}
export RELAY_EXTRA=${RELAY_EXTRA:-}

# Timeline: relay up, publisher at +3s, subscribers at +6s.
export PUB_SECS=$((DURATION + 15))
export RELAY_SECS=$((DURATION + 25))
if [ "$HEAVY_FRAC" != 0 ] && [ "$HEAVY_TRACKS" -gt "$TRACKS" ]; then
	export PUB_TRACKS=$HEAVY_TRACKS
else
	export PUB_TRACKS=$TRACKS
fi

if [ -n "${SLURM_JOB_NODELIST:-}" ] && [ "${LOCAL:-0}" != 1 ]; then
	mapfile -t nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
	[ "${#nodes[@]}" -ge 4 ] || { echo "need 4 nodes, have ${#nodes[@]}" >&2; exit 1; }
	relay_node=${nodes[0]} pub_node=${nodes[1]} sub_nodes=("${nodes[2]}" "${nodes[3]}")
	export RELAY_HOST=${RELAY_HOST:-$relay_node}
	on() { local node=$1; shift; srun --nodes=1 --ntasks=1 --nodelist="$node" --overlap "$@"; }
else
	relay_node=localhost pub_node=localhost sub_nodes=(localhost localhost)
	export RELAY_HOST=${RELAY_HOST:-127.0.0.1}
	on() { shift; "$@"; }
fi

[ -d "$OUT" ] && { echo "$OUT already exists" >&2; exit 1; }
mkdir -p "$OUT"
[ -f "$CERT_DIR/relay.crt" ] || "$here/make-cert.sh" "$CERT_DIR" "$RELAY_HOST"

# Record the configuration next to the results.
python3 - "$OUT/run.json" <<'PY'
import json, os, sys
keys = ["RUN_ID","SUBS","DURATION","BITRATE","FPS","GOP_SECS","TRACE","TRACKS","HEAVY_FRAC",
        "HEAVY_TRACKS","SLOW_FRAC","SLOW_MODE","STREAM_WIN","CONN_WIN","RESUB_MS","SEND_CAP_MBPS",
        "RELAY_EXTRA","RELAY_HOST","STATS_MS","RAMP_MS","PUB_TRACKS"]
cfg = {k: os.environ.get(k, "") for k in keys}
json.dump(cfg, open(sys.argv[1], "w"), indent=1)
PY
git -C "$repo" rev-parse HEAD > "$OUT/git-commit.txt" 2>/dev/null || true
git -C "$repo" status --short > "$OUT/git-status.txt" 2>/dev/null || true

echo "[$RUN_ID] relay=$relay_node pub=$pub_node subs=${sub_nodes[*]} subs=$SUBS slow=$SLOW_FRAC:$SLOW_MODE cap=${SEND_CAP_MBPS:-none}"

half=$((SUBS / 2))
pids=()
on "$relay_node" "$here/role.sh" relay & pids+=($!)
sleep 3
on "$pub_node" "$here/role.sh" pub & pids+=($!)
sleep 3
on "${sub_nodes[0]}" "$here/role.sh" sub 0 "$half" subA & pids+=($!)
on "${sub_nodes[1]}" "$here/role.sh" sub "$half" "$((SUBS - half))" subB & pids+=($!)

status=0
roles=(relay pub subA subB)
for i in "${!pids[@]}"; do
	wait "${pids[$i]}" || { rc=$?; status=$rc; echo "[$RUN_ID] ${roles[$i]} exited with $rc" >&2; }
done

python3 "$here/summarize.py" "$OUT" --append "$RESULTS/summary.csv" || status=$?
echo "[$RUN_ID] done (status $status) -> $OUT"
exit "$status"
