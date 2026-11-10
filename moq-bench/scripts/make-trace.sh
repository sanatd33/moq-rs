#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Extract a frame-size/timing trace from an encoded video for
# `moq-bench-pub --trace`. Re-encode first if you need a specific bitrate, e.g.
#   ffmpeg -i bbb.mp4 -an -c:v libx264 -b:v 4M -maxrate 4M -bufsize 8M -g 30 \
#          -keyint_min 30 -sc_threshold 0 -bf 0 bbb-4M.mp4
# (-g 30 at 30 fps gives the report's 1 s keyframe interval; -bf 0 keeps
# decode order equal to presentation order).
#
# usage: make-trace.sh <video> <out.csv>
set -euo pipefail
in=${1:?usage: make-trace.sh <video> <out.csv>}
out=${2:?usage: make-trace.sh <video> <out.csv>}
ffprobe -v error -select_streams v:0 -show_entries packet=dts_time,size,flags \
	-of csv=p=0 "$in" > "$out"
echo "wrote $(wc -l < "$out") packets to $out"
