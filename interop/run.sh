#!/usr/bin/env bash
# Runs the grpc-go interop suite against the Zig echo server.
#
# This is the check that matters for the "must be real gRPC on the wire"
# constraint: our own client agreeing with our own server proves only that we
# are self-consistent. grpc-go is the reference implementation.
#
# Requires: go, zig. Usage: interop/run.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==> building Zig echo server"
zig build

echo "==> building grpc-go interop client"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"; [[ -n "${server_pid:-}" ]] && kill "$server_pid" 2>/dev/null || true' EXIT
(cd interop && go build -o "$tmpdir/interop" .)

echo "==> starting server"
./zig-out/bin/echo_server >"$tmpdir/server.log" 2>&1 &
server_pid=$!

# Wait for the server to announce its port rather than sleeping a fixed amount.
port=""
for _ in $(seq 1 50); do
    if port="$(grep -oP 'listening \K[0-9]+' "$tmpdir/server.log" 2>/dev/null)" && [[ -n "$port" ]]; then
        break
    fi
    sleep 0.1
done
if [[ -z "$port" ]]; then
    echo "server failed to start:" >&2
    cat "$tmpdir/server.log" >&2
    exit 1
fi
echo "    server listening on 127.0.0.1:$port"

echo "==> running interop checks"
"$tmpdir/interop" "127.0.0.1:$port"
