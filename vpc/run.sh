#!/usr/bin/env bash
# Runs the in-VPC benchmark matrix from the client host and writes one results
# file. See vpc/README.md for setup. Usage:
#
#   vpc/run.sh <server-ip:port> <label> [path/to/vpc_bench]
#
#   REPS=3        repetitions of the whole matrix
#   DURATION=30   seconds per measurement
#   RATES="500 2000 5000"   open-loop total calls/s; keep them under what the
#                 closed-loop rows show the pair can sustain
#
# The label should say the placement, e.g. same-zone-<machine-type> or lan-wired.
set -euo pipefail

target="${1:?usage: run.sh <ip:port> <label> [vpc_bench]}"
label="${2:?usage: run.sh <ip:port> <label> [vpc_bench]}"
here="$(dirname "${BASH_SOURCE[0]}")"
# Next to this script (scp'd to a bare host), else the repo's build output.
if [ -x "$here/vpc_bench" ]; then default_bench="$here/vpc_bench"; else default_bench="$here/../zig-out/bin/vpc_bench"; fi
bench="${3:-$default_bench}"
[ -x "$bench" ] || { echo "vpc_bench not found at $bench; pass its path as the third argument" >&2; exit 1; }
reps="${REPS:-3}"
duration="${DURATION:-30}"
rates="${RATES:-500 2000 5000}"

host="${target%:*}"
out="vpc-$label-$(date -u +%Y%m%dT%H%M%SZ).txt"

{
    echo "# label $label"
    echo "# date $(date -u +%FT%TZ)"
    echo "# client $(uname -m) $(nproc) cpus, $(hostname)"
    echo
    echo "# network floor: ICMP RTT to $host (the budget the RPC cannot beat)"
    # Intervals under 200ms need root; fall back to passwordless sudo.
    if rtt=$(ping -c 200 -i 0.05 -q "$host" 2>&1 || sudo -n ping -c 200 -i 0.05 -q "$host" 2>&1); then
        echo "$rtt" | tail -2
    else
        echo "# ping unavailable: $(echo "$rtt" | tail -1)"
    fi
    echo
} | tee "$out"

run() { "$bench" --target "$target" --duration "$duration" --label "$label" "$@" | tee -a "$out"; }

for rep in $(seq 1 "$reps"); do
    echo "## rep $rep" | tee -a "$out"

    # One caller, one call at a time: the unloaded latency.
    for size in 64 1024 16384; do
        run --payload "$size" --callers 1
    done

    # Fixed offered load. These are the rows for p99/p999.
    for rate in $rates; do
        run --payload 1024 --callers 8 --rate "$rate"
    done

    # Saturation: what the pair sustains, and the latency at the knee.
    run --payload 1024 --callers 8
    run --payload 1024 --callers 64
    echo | tee -a "$out"
done

echo "wrote $out"
