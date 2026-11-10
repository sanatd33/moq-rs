#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Self-signed relay certificate (no Go/mkcert needed). Clients connect with
# --tls-disable-verify, so the SAN only has to be well formed.
#
# usage: make-cert.sh <out-dir> [hostname...]
set -euo pipefail
out=${1:?usage: make-cert.sh <out-dir> [hostname...]}
shift
mkdir -p "$out"
san="DNS:localhost,IP:127.0.0.1"
for h in "$@"; do san="$san,DNS:$h"; done
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
	-subj "/CN=moq-bench-relay" -addext "subjectAltName=$san" \
	-keyout "$out/relay.key" -out "$out/relay.crt" 2>/dev/null
echo "wrote $out/relay.crt $out/relay.key ($san)"
