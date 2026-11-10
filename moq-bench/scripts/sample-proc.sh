#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Sample a process's memory and CPU, plus host NIC counters, from /proc (Linux).
# Runs until the process exits.
#
# usage: sample-proc.sh <pid> <out.csv> [interval_seconds]
set -euo pipefail
pid=${1:?usage: sample-proc.sh <pid> <out.csv> [interval]}
out=${2:?usage: sample-proc.sh <pid> <out.csv> [interval]}
interval=${3:-1}
tck=$(getconf CLK_TCK)

echo "unix_ms,rss_kb,hwm_kb,utime_ticks,stime_ticks,threads,clk_tck,net_rx_bytes,net_tx_bytes" > "$out"
while kill -0 "$pid" 2>/dev/null; do
	ts=$(date +%s%3N)
	mem=$(awk '/^VmRSS/{r=$2} /^VmHWM/{h=$2} END{print r","h}' "/proc/$pid/status" 2>/dev/null) || break
	# Fields after the parenthesised comm, which may contain spaces.
	cpu=$(sed 's/^.*) //' "/proc/$pid/stat" 2>/dev/null | awk '{print $12","$13","$18}') || break
	net=$(awk 'NR>2 { sub(/:/, " "); if ($1 != "lo") { rx += $2; tx += $10 } } END { print rx","tx }' /proc/net/dev)
	echo "$ts,$mem,$cpu,$tck,$net" >> "$out"
	sleep "$interval"
done
