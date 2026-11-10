#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Summarize one run directory written by run-one.sh into a single row.

Standard library only, so it runs on cluster nodes without extra packages.

Steady-state window: from WARMUP seconds after the first subscriber object
until TAIL seconds before the last one. Joining subscribers first receive the
current group from its start, which inflates early latency, so the warm-up is
excluded from every latency and throughput figure.

usage: summarize.py <run-dir> [--append summary.csv] [--warmup 10] [--tail 3]
"""

import argparse
import csv
import glob
import json
import math
import os
import sys
from collections import defaultdict


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(round((len(sorted_vals) - 1) * p)))]


def jain(xs):
    xs = [x for x in xs if x is not None]
    if not xs or sum(xs) == 0:
        return None
    return sum(xs) ** 2 / (len(xs) * sum(x * x for x in xs))


def read_jsonl(path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def is_recovering_stall(mode):
    # stall:<after>:<duration> recovers; those terminations are unnecessary.
    return mode.startswith("stall:") and mode.count(":") == 2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--append", help="append the summary row to this CSV")
    ap.add_argument("--warmup", type=float, default=10.0)
    ap.add_argument("--tail", type=float, default=3.0)
    args = ap.parse_args()
    d = args.run_dir

    cfg = json.load(open(os.path.join(d, "run.json")))
    row = {k: cfg.get(k, "") for k in
           ["RUN_ID", "SUBS", "DURATION", "TRACKS", "HEAVY_FRAC", "HEAVY_TRACKS",
            "SLOW_FRAC", "SLOW_MODE", "SEND_CAP_MBPS", "STREAM_WIN", "CONN_WIN", "TRACE"]}

    # Session modes and per-interval throughput.
    mode = {}
    tracks = {}
    intervals = []
    for path in glob.glob(os.path.join(d, "sub*-intervals.jsonl")):
        for r in read_jsonl(path):
            mode[r["session"]] = r["mode"]
            tracks[r["session"]] = r["tracks"]
            intervals.append(r)

    # Per-object latency; also defines the steady window.
    objects = glob.glob(os.path.join(d, "sub*-objects.csv"))
    recv = []
    for path in objects:
        with open(path) as f:
            for r in csv.DictReader(f):
                recv.append((int(r["recv_us"]), int(r["session"]), int(r["seq"]),
                             int(r["recv_us"]) - int(r["send_us"])))
    if recv:
        t_first = min(r[0] for r in recv) / 1e6
        t_last = max(r[0] for r in recv) / 1e6
    elif intervals:
        t_first = min(r["unix_ms"] for r in intervals) / 1e3
        t_last = max(r["unix_ms"] for r in intervals) / 1e3
    else:
        print(f"{d}: no subscriber output", file=sys.stderr)
        return 1
    lo, hi = t_first + args.warmup, t_last - args.tail
    row["steady_secs"] = round(hi - lo, 1)

    def healthy(s):
        return mode.get(s, "normal") == "normal" and tracks.get(s) == min(tracks.values())

    # Throughput per session over the steady window.
    bytes_by = defaultdict(int)
    for r in intervals:
        if lo <= r["unix_ms"] / 1e3 <= hi:
            bytes_by[r["session"]] += r["bytes"]
    secs = max(hi - lo, 1e-9)
    mbps = {s: bytes_by.get(s, 0) * 8 / secs / 1e6 for s in mode}
    # Raw per-session throughput: the isolation goal is an equal per-session
    # share, so a multi-track session counts once, like any other session.
    row["jain_all"] = jain(list(mbps.values()))
    row["jain_healthy"] = jain([v for s, v in mbps.items() if healthy(s)])
    h = sorted(v for s, v in mbps.items() if healthy(s))
    row["healthy_mbps_mean"] = sum(h) / len(h) if h else None
    row["healthy_mbps_min"] = h[0] if h else None
    heavy = [v for s, v in mbps.items() if tracks.get(s, 0) > min(tracks.values())]
    row["heavy_mbps_mean"] = sum(heavy) / len(heavy) if heavy else None
    slow = [v for s, v in mbps.items() if mode.get(s, "normal") != "normal"]
    row["slow_mbps_mean"] = sum(slow) / len(slow) if slow else None
    row["aggregate_mbps"] = sum(mbps.values())

    # Latency and delivery over the steady window.
    lat = defaultdict(list)
    seqs_by = defaultdict(set)
    sent_at = {}
    last_recv = defaultdict(int)
    max_lat = defaultdict(int)
    for t, s, seq, l in recv:
        seqs_by[s].add(seq)
        sent_at[seq] = t - l
        last_recv[s] = max(last_recv[s], t)
        max_lat[s] = max(max_lat[s], l)
        if lo <= t / 1e6 <= hi:
            key = "healthy" if healthy(s) else ("slow" if mode.get(s, "normal") != "normal" else "heavy")
            lat[key].append(l)
    for key in ["healthy", "slow", "heavy"]:
        v = sorted(lat[key])
        row[f"{key}_lat_p50_ms"] = pct(v, 0.5) / 1000 if v else None
        row[f"{key}_lat_p99_ms"] = pct(v, 0.99) / 1000 if v else None
    def gap_frac(s):
        # Frames the relay skipped never arrive; backlogged ones arrive late.
        # Expect only frames sent after warm-up and early enough that the
        # session's worst observed latency would have delivered them before it
        # stopped, so the end-of-run backlog is not counted as loss.
        v = seqs_by.get(s)
        if not v:
            return 1.0
        cutoff = last_recv[s] - max_lat[s] - 1_000_000
        expected = [q for q, at in sent_at.items() if lo * 1e6 <= at <= cutoff]
        if not expected:
            return None
        return sum(1 for q in expected if q not in v) / len(expected)

    for key, pred in [("healthy", healthy),
                      ("slow", lambda s: mode.get(s, "normal") != "normal")]:
        g = [x for x in (gap_frac(s) for s in mode if pred(s)) if x is not None]
        row[f"{key}_frames_missing_max"] = max(g) if g else None

    # Terminations.
    term = defaultdict(int)
    term_codes = defaultdict(int)
    errors = 0
    for path in glob.glob(os.path.join(d, "sub*-events.jsonl")):
        for e in read_jsonl(path):
            if e["event"] == "subscription_terminated":
                m = mode.get(e["session"], "normal")
                term["recovering" if is_recovering_stall(m) else ("slow" if m != "normal" else "healthy")] += 1
                term_codes[e.get("code")] += 1
            elif e["event"] in ("session_error", "subscription_error", "session_closed"):
                errors += 1
    row["terminations_healthy"] = term["healthy"]
    row["terminations_slow"] = term["slow"]
    row["terminations_unnecessary"] = term["recovering"]
    row["termination_codes"] = ";".join(f"{k}:{v}" for k, v in sorted(term_codes.items(), key=str))
    row["session_errors"] = errors

    # Relay instrumentation.
    stats_path = os.path.join(d, "relay-stats.jsonl")
    if os.path.exists(stats_path):
        rs = [r for r in read_jsonl(stats_path) if lo <= r["unix_ms"] / 1e3 <= hi]
        if rs:
            q = [r["total_queued_bytes"] for r in rs]
            u = [r["unique_referenced_bytes"] for r in rs]
            row["relay_queued_mb_peak"] = max(q) / 1e6
            row["relay_queued_mb_mean"] = sum(q) / len(q) / 1e6
            row["relay_cache_ref_mb_peak"] = max(u) / 1e6
            row["relay_max_lag_groups"] = max(
                (s["max_lag_groups"] for r in rs for s in r["sessions"]), default=0)
            row["relay_too_far_behind"] = max(
                (sum(s["too_far_behind"] for s in r["sessions"]) for r in rs), default=0)
            row["relay_send_wait_s"] = (rs[-1].get("send_limit_wait_ms") or 0) / 1000

    proc_path = os.path.join(d, "relay-proc.csv")
    if os.path.exists(proc_path):
        with open(proc_path) as f:
            ps = [r for r in csv.DictReader(f) if r["rss_kb"]]
        steady = [r for r in ps if lo <= int(r["unix_ms"]) / 1e3 <= hi]
        if steady:
            rss = [int(r["rss_kb"]) for r in steady]
            row["relay_rss_mb_peak"] = max(rss) / 1024
            row["relay_rss_mb_mean"] = sum(rss) / len(rss) / 1024
            a, b = steady[0], steady[-1]
            dt = (int(b["unix_ms"]) - int(a["unix_ms"])) / 1e3
            if dt > 0:
                ticks = (int(b["utime_ticks"]) + int(b["stime_ticks"])
                         - int(a["utime_ticks"]) - int(a["stime_ticks"]))
                row["relay_cpu_cores"] = ticks / int(b["clk_tck"]) / dt
                row["relay_tx_mbps"] = (int(b["net_tx_bytes"]) - int(a["net_tx_bytes"])) * 8 / dt / 1e6

    # Generator headroom: a saturated load generator invalidates the run.
    for path in glob.glob(os.path.join(d, "sub*-proc.csv")):
        label = os.path.basename(path)[: -len("-proc.csv")]
        with open(path) as f:
            ps = [r for r in csv.DictReader(f) if r["utime_ticks"]]
        steady = [r for r in ps if lo <= int(r["unix_ms"]) / 1e3 <= hi]
        if len(steady) > 1:
            a, b = steady[0], steady[-1]
            dt = (int(b["unix_ms"]) - int(a["unix_ms"])) / 1e3
            ticks = (int(b["utime_ticks"]) + int(b["stime_ticks"])
                     - int(a["utime_ticks"]) - int(a["stime_ticks"]))
            row[f"{label}_cpu_cores"] = ticks / int(b["clk_tck"]) / dt

    def fmt(v):
        if isinstance(v, float):
            return "" if math.isnan(v) else f"{v:.4g}"
        return "" if v is None else v

    out = {k: fmt(v) for k, v in row.items()}
    print(json.dumps(out, indent=1))
    if args.append:
        new = not os.path.exists(args.append)
        if not new:
            with open(args.append) as f:
                header = next(csv.reader(f), [])
            # Columns vary with the scenario; keep the file's header and
            # widen it only when this run adds columns.
            fields = header + [k for k in out if k not in header]
            if fields != header:
                with open(args.append) as f:
                    old = list(csv.DictReader(f))
                with open(args.append, "w", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fields)
                    w.writeheader()
                    w.writerows(old)
        else:
            fields = list(out)
        with open(args.append, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if new:
                w.writeheader()
            w.writerow(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
