# Project: zgrpc — a real gRPC client/server library for Zig

## What this is

A standalone gRPC library for Zig: actual gRPC-over-HTTP/2 wire protocol,
not Twirp, not a custom TCP protocol. Built because no mature option exists
yet in the Zig ecosystem — most prior art is either C-interop glue around
the gRPC C core, or protobuf codegen with no transport layer at all.

This is a deliverable in its own right, not an internal detail of any one
consumer. The time series DB (separate repo, depends on this one) is the
first real consumer, but the API should stay general enough for others.

## Non-negotiable constraints

- **Must be real gRPC on the wire** — HTTP/2 framing, standard gRPC
  message framing, `grpc-status`/`grpc-message` trailers, `:path` =
  `/package.Service/Method`. Existing external systems generate stubs from
  shared `.proto` files and expect standard semantics — this library needs
  to interoperate with them, not just with itself.
- **Latency target: ~5ms** for unary calls, in-VPC. This is a p50 *and*
  tail-latency (p99/p999) concern, not just an average.
- **h2c (plaintext HTTP/2), no TLS.** All current consumers run inside a
  single trusted VPC. Confirm this assumption still holds before any
  consumer relies on it outside that context — TLS/ALPN support can be
  added later if a use case needs it, but isn't in scope now.

## Scope discipline

Build only what's needed to validate the approach and serve the first real
consumer. Specifically avoid over-building before there's a second
consumer to validate the abstractions against:

- Unary RPCs first. Server streaming once unary is proven and benchmarked.
- No interceptors, no retries, no client-side load balancing, no service
  discovery. These are real gRPC features but nobody needs them yet.
- No TLS/ALPN until a concrete use case requires leaving the VPC.

## Architecture (layers, bottom to top)

1. **TCP/h2c** — plaintext transport.
2. **HTTP/2** — do not hand-roll. Evaluate and pick one:
   - `hendriknielaender/http2.zig` — connection preface, binary framing,
     stream multiplexing, flow control, HPACK, GOAWAY/SETTINGS/PING.
   - `httpx.zig` (muhammad-fiaz) — from-scratch HTTP/2 + HTTP/3, HPACK/QPACK.
   Both are new as of 2026 and unproven at production tail-latency
   scrutiny — this is the primary technical risk in the project. Whichever
   is chosen, expect to read its source, not just its docs, when tail
   latency doesn't match expectations.
3. **gRPC framing** — the part actually written here: 5-byte header
   (1-byte compression flag + 4-byte big-endian length) + serialized
   protobuf payload, plus trailer/status handling and deadline
   (`grpc-timeout`) propagation.
4. **Protobuf messages** — `Arwalk/zig-protobuf` for encode/decode and
   service interface codegen from `.proto` files. Use its `StreamDecoder`
   (zero-allocation pull parser over `std.Io.Reader`) to decode straight
   off the HTTP/2 stream instead of buffering full messages — this matters
   for the tail-latency budget, not just throughput.

## Dependency versions (confirmed working)

- **Zig: 0.16.0** (current stable). Installed/managed via `zvm`
  (hendriknielaender/zvm) — active version resolves through the
  `~/.local/share/.zm/bin` symlink, which `zvm use` repoints.
- **zig-protobuf: pin to the `v5.0.0` tag, not `#master`.**
  - Confirmed: `git checkout v5.0.0` + `zig build test` passes cleanly
    against Zig 0.16.0, full suite, no modifications needed.
  - Do NOT use `#master` — as of this check, `master`'s `build.zig`
    references `tests/protos_for_test/onnx.proto`, which does not exist
    anywhere in that branch's history (`git log --all` for the path
    returns nothing on `master`; confirmed present at the `v5.0.0` tag).
    This breaks `zig build test` on a plain clone of `master` with:
    `error: Could not make proto path relative: .../onnx.proto: No such
    file or directory`. This is upstream repo state, not a local/clone
    issue or a 0.16 compatibility problem — the library itself builds and
    tests fine, only `master`'s test-fixture list is currently broken.
  - Dependency line to use:
    `zig fetch --save "git+https://github.com/Arwalk/zig-protobuf#v5.0.0"`
  - Worth periodically checking https://github.com/Arwalk/zig-protobuf/issues
    for whether this has been fixed on `master` before ever switching off
    the pinned tag.

## Milestones (do in this order)

1. **Proof of concept**: single unary gRPC call working end-to-end, real
   framing, one HTTP/2 library candidate, h2c, one persistent connection.
2. **Benchmark** p50/p99/p999 for a realistic payload size, under load, in
   the actual target VPC — not localhost, since same-AZ vs cross-AZ RTT
   changes the budget meaningfully. A bad tail here almost certainly means
   the HTTP/2 library (buffering, HPACK table churn, flow-control window
   defaults), not the framing code above it — isolate before optimizing.
3. **Server streaming support**, once unary is solid — needed by the TSDB
   consumer for range queries that shouldn't buffer fully in memory.
4. **Harden**: connection lifecycle (long-lived, no per-request reconnect),
   error/status code mapping, deadline propagation, basic connection pool.
5. **Publish as a versioned dependency** once the API is stable enough for
   the TSDB repo to depend on it via git ref instead of a local path.

## Open questions

- Which HTTP/2 library wins the milestone-2 benchmark? Record the decision
  and the numbers once made — this is the single most consequential choice
  in the project.
- What does the public Zig API look like for defining a service and
  registering handlers? Sketch this early since the TSDB repo will be the
  first (and for a while, only) real consumer of it.
- Connection pooling / concurrency model: how many persistent HTTP/2
  connections per peer, and what's the backpressure story under load?

## Commands

- `zig build` — build the library and `examples/echo_server.zig`
- `zig build test` — unit + integration tests
- `zig build test-unit` — pure unit tests only (no sockets)
- `zig build test-integration` — real client/server over loopback
- `zig build bench` — loopback latency baseline (NOT the milestone-2 number)
- `zig build bench-build` / `zig build test-integration-build` — compile only
- `./interop/run.sh` — grpc-go interop suite; requires Go

## Decisions made

**HTTP/2 library: `hendriknielaender/http2.zig`, frame codec + HPACK only.**
The gRPC stream sequence is driven directly by `src/h2.zig`. Neither candidate's
high-level API can express gRPC trailers: `http2.zig` guards response headers
with `if (stream.response_headers_sent) return;` and puts END_STREAM on the
final DATA frame, and `httpx.zig` exposes HTTP/2 only through a route-table
server API whose only trailer support is HTTP/1.1 chunked. Full comparison and
rationale in README.md. Note the dependency's own `zig build` fails on its
BoringSSL TLS targets; the `http2` module itself is BoringSSL-free.

**Public API shape**: handlers take and return `[]const u8`, with a
`CallContext` carrying the per-call arena, path, deadline and settable status.
Protobuf codegen sits above the transport rather than inside it.

**Concurrency model**: a unit of concurrency per connection, per connection a
reader task that owns the read side, and per stream a task running the call.
Writers take a separate lock from connection state and the two are never held
at once, so a blocked `flush` cannot stall frame processing. A connection's
header blocks are decoded only on its reader task — upstream's HPACK scratch
buffer is `threadlocal` (not process-wide, as previously recorded here), so
concurrency across connections is safe and interleaving within one is not.
Measured cost: about 11us of p50 per call for the thread handoff, against a
~5ms budget. Full numbers and the two bugs it flushed out in README "Latency".

**Flow-control windows**: 256KB per stream, 1MB per connection (raised by a
WINDOW_UPDATE on stream 0, since SETTINGS cannot set it). Credit is returned on
consumption, not arrival — the connection window is then the bound on what a
peer can make one connection buffer.

**Server streaming (milestone 3) is done**: `Method.serverStream` +
`ServerStream.send` on the server, `callServerStream` + `ResponseStream.next`
on the client, verified against grpc-go. Messages are not buffered whole at
either end.

Prompt stream cancellation is done: the reader task records RST_STREAM on
arrival, so `stream.send` fails at the next message whatever the flow-control
window is doing. Verified by an integration test in which the handler is parked
with a wide-open window.

Still open: connection pooling (the client is still one connection per
`Client`), the milestone-2 in-VPC benchmark numbers, and a clean loopback
re-measurement — the last one was taken on a loaded machine and only the A/B
between old and new transports is trustworthy.
