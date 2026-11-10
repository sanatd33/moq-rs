#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# One experiment role, run on its own node by run-one.sh. Configuration comes
# from the environment run-one.sh exports. Output is written to node-local
# disk during the run and copied to $OUT afterwards, so shared-filesystem
# latency cannot perturb the measurement.
#
# usage: role.sh relay | pub | sub <index-offset> <sessions> <label>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
role=${1:?role}
# Per role, so roles sharing a machine (LOCAL=1) never clean up each other.
local_dir=${TMPDIR:-/tmp}/moqbench-$USER/$RUN_ID/$role-${4:-0}
mkdir -p "$local_dir" "$OUT"

hostinfo() {
	{
		echo "role=$1 host=$(hostname) date=$(date -Is)"
		uname -a
		lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|Thread|Socket|MHz' || true
		free -g 2>/dev/null || true
		sysctl net.core.rmem_max net.core.wmem_max 2>/dev/null || true
		ip -br addr 2>/dev/null || true
		chronyc tracking 2>/dev/null || timedatectl show 2>/dev/null || echo "no chrony/timedatectl"
	} > "$local_dir/hostinfo-$1.txt" 2>&1
}

# Stop the /proc sampler before copying, so it cannot append after cleanup.
stop_sampler() {
	kill "$1" 2>/dev/null || true
	wait "$1" 2>/dev/null || true
}

finish() {
	cp -r "$local_dir"/. "$OUT"/
	rm -rf "$local_dir"
}

case "$role" in
relay)
	hostinfo relay
	args=(--bind "0.0.0.0:$PORT" --tls-cert "$CERT_DIR/relay.crt" --tls-key "$CERT_DIR/relay.key"
		--coordinator-file "$local_dir/coordinator.json"
		--bench-stats-file "$local_dir/relay-stats.jsonl" --bench-stats-interval-ms "$STATS_MS")
	if [ -n "${SEND_CAP_MBPS:-}" ]; then
		args+=(--send-rate-limit-mbps "$SEND_CAP_MBPS")
	fi
	# shellcheck disable=SC2086
	RUST_LOG=${RELAY_LOG:-warn} "$BIN/moq-relay-ietf" "${args[@]}" ${RELAY_EXTRA:-} \
		> "$local_dir/relay.log" 2>&1 &
	relay=$!
	"$here/sample-proc.sh" "$relay" "$local_dir/relay-proc.csv" 1 &
	sampler=$!
	sleep "$RELAY_SECS"
	kill -TERM "$relay" 2>/dev/null || true
	wait "$relay" || true
	stop_sampler "$sampler"
	finish
	;;
pub)
	hostinfo pub
	args=("https://$RELAY_HOST:$PORT" --tls-disable-verify --tracks "$PUB_TRACKS"
		--bitrate-mbps "$BITRATE" --fps "$FPS" --gop-secs "$GOP_SECS" --duration-secs "$PUB_SECS")
	if [ -n "${TRACE:-}" ]; then
		args+=(--trace "$TRACE")
	fi
	"$BIN/moq-bench-pub" "${args[@]}" > "$local_dir/pub.log" 2>&1 &
	pub=$!
	"$here/sample-proc.sh" "$pub" "$local_dir/pub-proc.csv" 1 &
	sampler=$!
	wait "$pub" || echo "publisher exited with $?" >> "$local_dir/pub.log"
	stop_sampler "$sampler"
	finish
	;;
sub)
	offset=${2:?offset} sessions=${3:?sessions} label=${4:?label}
	hostinfo "$label"
	ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
	args=("https://$RELAY_HOST:$PORT" --tls-disable-verify
		--sessions "$sessions" --index-offset "$offset" --label "$label"
		--tracks "$TRACKS" --heavy-fraction "$HEAVY_FRAC" --heavy-tracks "$HEAVY_TRACKS"
		--slow-fraction "$SLOW_FRAC" --slow-mode "$SLOW_MODE"
		--duration-secs "$DURATION" --ramp-ms "$RAMP_MS"
		--stats-interval-ms "$STATS_MS" --out-dir "$local_dir")
	[ -n "${STREAM_WIN:-}" ] && args+=(--stream-recv-window-bytes "$STREAM_WIN")
	[ -n "${CONN_WIN:-}" ] && args+=(--recv-window-bytes "$CONN_WIN")
	[ -n "${RESUB_MS:-}" ] && args+=(--resubscribe-ms "$RESUB_MS")
	[ "${OBJECT_LOG:-1}" = 0 ] && args+=(--no-object-log)
	"$BIN/moq-bench-sub" "${args[@]}" > "$local_dir/$label.log" 2>&1 &
	sub=$!
	"$here/sample-proc.sh" "$sub" "$local_dir/$label-proc.csv" 1 &
	sampler=$!
	wait "$sub" || echo "subscriber exited with $?" >> "$local_dir/$label.log"
	stop_sampler "$sampler"
	finish
	;;
*)
	echo "unknown role $role" >&2
	exit 2
	;;
esac
