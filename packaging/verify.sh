#!/usr/bin/env bash
#
# Verifies zgrpc works as a *published dependency*, not merely as a checkout.
#
# These are different things, and the difference is where packaging breaks. A
# consumer gets only the files listed in `build.zig.zon`'s `paths` — no
# `examples/`, no `interop/`, no tests it can run — and it runs zgrpc's
# `build.zig` as a dependency rather than as the root package. Something that
# builds fine here can still be unusable there.
#
# So: build a tarball of the working tree exactly as a git ref would produce,
# fetch it into a throwaway project by URL and hash, and make a real call.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "==> packing the working tree"
# HEAD, not the dirty tree: this is what a consumer fetching a tag would get,
# and uncommitted files are not part of any tag. ZGRPC_REF names another
# tree-ish, for checking a change before it is committed.
git -C "$root" archive --format=tar.gz --prefix=zgrpc/ -o "$work/zgrpc.tar.gz" "${ZGRPC_REF:-HEAD}"

echo "==> preparing a consumer project"
cp -R "$root/packaging/consumer" "$work/consumer"

echo "==> fetching zgrpc as a dependency"
(cd "$work/consumer" && zig fetch --save "file://$work/zgrpc.tar.gz")

echo "==> what the consumer actually received"
tar tzf "$(find "${ZIG_GLOBAL_CACHE_DIR:-$HOME/.cache/zig}/p" -maxdepth 1 -name 'zgrpc-*.tar.gz' -newer "$work/zgrpc.tar.gz" | head -1)" |
    sed 's|^[^/]*/|  |' || true

echo "==> building and running against it"
(cd "$work/consumer" && zig build run)
