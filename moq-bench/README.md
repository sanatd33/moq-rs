# moq-bench

Load generators and harness for the relay resource-isolation experiments
(slow subscribers, bandwidth contention). Built on the draft-16 relay
(`3b2b584`) with measurement-only additions. Every variant (baseline, DRR,
DRR + limits) must be built from a branch that carries these same additions,
so instrumentation overhead is identical across comparisons.

## What is added to the relay and transport

All of this is off unless a flag enables it.

| Flag (moq-relay-ietf) | Effect |
|---|---|
| `--bench-stats-file F` | Appends a JSON line every `--bench-stats-interval-ms` (default 1000) with per-session bytes sent, objects sent, queued bytes, in-flight subgroups, lag (groups) and TOO_FAR_BEHIND count, plus relay-wide queued bytes and de-duplicated cache bytes. |
| `--send-rate-limit-mbps M` | Shared FIFO token bucket on all downstream subgroup payload (`--send-rate-burst-bytes`, default 64 KiB). This is the testbed's bandwidth bottleneck, not part of any fix. |

How "queued" is measured: the relay has no per-subscriber queue. Each
downstream subscription runs one task per in-flight subgroup (at most 128),
each holding a reader into the shared cache. Queued bytes = payload present in
those cached subgroups that the subscription has not yet handed to QUIC.
Bytes already inside quinn's send buffers are not visible to it.
`unique_referenced_bytes` counts each shared cache entry once, and the
`/proc` RSS sample gives total process memory.

Client-side additions, used only by `moq-bench-sub`:
- `Subscriber::set_recv_throttle`: paces data-stream reads so a slow client
  stops reading from QUIC and flow control pushes back on the relay.
- `TrackWriter::deliver_all_subgroups`: by default the client cache only
  exposes the newest subgroup and cancels any it skipped, which tears down
  the subscription after a burst. The bench client opts out of that so it
  measures the relay, not the client library.
- `quic::Config::with_client_receive_windows`: small receive windows, so
  backpressure reaches the relay quickly.

## Binaries

- `moq-bench-pub`: synthetic stream (30 fps, 4 Mb/s, keyframe 10x a delta frame,
  1 s GOP, one group per GOP) or `--trace` replay. Every object starts with a
  24-byte header carrying a sequence number and the send time.
- `moq-bench-sub`: N sessions, each with its own QUIC connection. Each session is
  `normal`, or one of the slow modes `rate:<mbps>`, `pause:<on_ms>:<off_ms>` or
  `stall:<after_ms>[:<duration_ms>]`, chosen by `--slow-fraction`. A
  `--heavy-fraction` of sessions subscribe to `--heavy-tracks` tracks.
  Outputs `<label>-objects.csv`, `<label>-intervals.jsonl` and
  `<label>-events.jsonl`.

## Running on PACE ICE

```sh
# once, on a compute node so glibc matches
curl https://sh.rustup.rs -sSf | sh -s -- -y
cargo build --release -p moq-relay-ietf -p moq-bench

# preflight (calibrate = generator headroom check at 384 and 768 sessions)
sbatch moq-bench/scripts/job.sbatch calibrate
# baseline sweeps
sbatch moq-bench/scripts/job.sbatch bandwidth baseline
sbatch moq-bench/scripts/job.sbatch slow baseline
sbatch moq-bench/scripts/job.sbatch heavy baseline
```

Results land in `bench-results/<run-id>/`, with one row per run appended to
`bench-results/summary.csv`. Before trusting results, check:
- `hostinfo-*.txt`: same CPU model/NIC on all nodes, and the clock-sync state.
  Latency is one-way across nodes, so it is only as good as the clock sync.
- `subA_cpu_cores` / `subB_cpu_cores` in the summary: well under the node's
  core count, or the generator (not the relay) is the bottleneck.
- `session_errors` is 0.
- Network: run `iperf3` between the relay and subscriber nodes to confirm at
  least 10 GbE before the 256-subscriber runs. That is not scripted, because
  iperf3 availability varies by cluster.

Single configurations: `SUBS=128 SLOW_FRAC=0.25 SLOW_MODE=rate:1 RUN_ID=x
moq-bench/scripts/run-one.sh` (inside an allocation, e.g. `salloc -N4`).
`LOCAL=1` runs every role on one machine for debugging. On a Mac it works, but
`/proc` samples are empty.

Media replay: `make-trace.sh bbb-4M.mp4 bbb.csv`, then `TRACE=$PWD/bbb.csv`
(see the script header for the ffmpeg encode).

## Summary columns

Steady window = 10 s after the first object until 3 s before the last. Joining
subscribers first get the current group from its start, so the warm-up is
excluded.

- `jain_all`, `jain_healthy`: Jain's index over per-session throughput.
- `healthy_lat_p50_ms` / `_p99_ms`: one-way object latency of normal,
  single-track sessions (also `slow_*`, `heavy_*`).
- `*_frames_missing_max`: worst per-session fraction of frames never received,
  ignoring frames still in the end-of-run backlog.
- `terminations_unnecessary`: subscriptions the relay ended for sessions
  whose stall would have recovered (`stall:<after>:<duration>`).
- `relay_queued_mb_*`, `relay_cache_ref_mb_peak`, `relay_rss_mb_*`,
  `relay_cpu_cores`, `relay_tx_mbps`.
- `relay_send_wait_s`: summed waiter-seconds on the send cap (total, not per
  session).
