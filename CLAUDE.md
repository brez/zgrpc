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
4. **Protobuf messages** — `Arwalk/zig-protobuf`, above the transport and not
   a dependency of it; see "Decisions made". Two corrections to what was
   assumed here, both checked against the v5.0.0 source:
   - There is no `StreamDecoder`. The name appears nowhere in the package.
     What exists is `protobuf.decode(T, reader, allocator)` over a
     `*std.Io.Reader`, which does allocate. Decoding straight off the HTTP/2
     stream rather than buffering the message would need something that does
     not exist yet, so zgrpc hands handlers a complete `[]const u8`.
   - "Service interface codegen" overstates it. protoc emits a vtable type per
     `service` — function pointers plus `package` and `service_name`
     constants — and zig-protobuf's own README says it is "service interfaces
     only ... does not include a gRPC transport layer". The signatures are not
     zgrpc's handler shape, so nothing plugs in: method paths and their types
     are written by hand. A protoc plugin emitting zgrpc handlers is the thing
     that would fix this.

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
   *(Done: v0.1.0.)*

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
- `zig build bench` — loopback latency baseline (NOT the milestone-2 number),
  plus pool throughput under load
- `zig build bench-build` / `zig build test-integration-build` — compile only
- `zig build flood` — build `reset_flood`, the rapid-reset load tool (run it
  against `vpc_server`; see docs/notes.md)
- `./interop/run.sh` — grpc-go interop suite; requires Go
- `./packaging/verify.sh` — fetch and use zgrpc as a published dependency
- `cd examples/protobuf && zig build run` — protobuf end to end; a standalone
  project so that zgrpc itself keeps having no protobuf dependency

## Decisions made

**HTTP/2 library: `hendriknielaender/http2.zig`, frame codec + HPACK only.**
The gRPC stream sequence is driven directly by `src/h2.zig`. Neither candidate's
high-level API can express gRPC trailers: `http2.zig` guards response headers
with `if (stream.response_headers_sent) return;` and puts END_STREAM on the
final DATA frame, and `httpx.zig` exposes HTTP/2 only through a route-table
server API whose only trailer support is HTTP/1.1 chunked. The full evaluation
— the two side by side, and why `httpx.zig` lost despite being the
better-maintained one — was dropped from the README once the decision stopped
being news; it is in that file at `1450ccd`, the last commit before it went.
Note the dependency's own `zig build` fails on its BoringSSL TLS targets; the
`http2` module itself is BoringSSL-free.

**Public API shape**: handlers take and return `[]const u8`, with a
`CallContext` carrying the per-call arena, io, path, deadline and settable
status. Protobuf codegen sits above the transport rather than inside it — and
as of v0.1.0 zgrpc has **no protobuf dependency at all**. It was wired into
every module in build.zig and imported nowhere; a consumer that wants generated
types depends on `Arwalk/zig-protobuf` directly rather than inheriting our pin.
The pinning notes above still apply to that consumer, which is why they stay
here.

**Concurrency model**: a unit of concurrency per connection, per connection a
reader task that owns the read side, and per stream a task running the call.
Writers take a separate lock from connection state and the two are never held
at once, so a blocked `flush` cannot stall frame processing. A connection's
header blocks are decoded only on its reader task — upstream's HPACK scratch
buffer is `threadlocal` (not process-wide, as previously recorded here), so
concurrency across connections is safe and interleaving within one is not.
Measured cost: about 8us of p50 per call for the thread handoff, against a
~5ms budget. Full numbers and the two bugs it flushed out in docs/notes.md.

**Flow-control windows**: 256KB per stream, 1MB per connection (raised by a
WINDOW_UPDATE on stream 0, since SETTINGS cannot set it). Credit is returned on
consumption, not arrival — the connection window is then the bound on what a
peer can make one connection buffer.

**Connection pooling**: `Pool` holds several connections to one peer above
`Client`, which stays one connection. Ceiling of 4 by default, grown lazily —
a connection is opened only when every existing one is busy, so a sequential
caller holds exactly one however high the ceiling. Idle first, then grow, then
share the least loaded. Retired connections finish their calls before closing;
no retries. Worth 25-37% throughput and 27-42% off p50 at 8 and 64 concurrent
callers on loopback (`zig build bench`, "under load"; numbers in docs/notes.md), which is where the
default came from — re-measure on the target host, since the useful ceiling
tracks cores.

**Deadlines are enforced, not just propagated.** `CallOptions.deadline_ns` goes
out as `grpc-timeout` and bounds every wait the call makes on both sides
(finding a stream, flow-control credit, waiting for frames), failing with
`error.DeadlineExceeded` — deliberately an error rather than a
`.deadline_exceeded` status, which means the *server* gave up. Needed a small
`Signal` type in h2.zig, because `std.Io.Condition` has no timed wait and
`futexWaitTimeout` does; measured no latency cost against the conditions it
replaced. Server clock starts at header receipt, on `.awake` so NTP cannot
extend a call. What is not covered: a handler burning CPU (no preemption),
connection establishment, and a socket write to a peer that grants HTTP/2
credit but stops draining TCP.

**Server streaming (milestone 3) is done**: `Method.serverStream` +
`ServerStream.send` on the server, `callServerStream` + `ResponseStream.next`
on the client, verified against grpc-go. Messages are not buffered whole at
either end.

Prompt stream cancellation is done: the reader task records RST_STREAM on
arrival, so `stream.send` fails at the next message whatever the flow-control
window is doing. Verified by an integration test in which the handler is parked
with a wide-open window.

**Milestone 5 (publishing) is done** at v0.1.0. `packaging/verify.sh` is the
thing to keep working: it packs HEAD the way a git ref would, fetches it into a
throwaway project and makes real calls, because building *in* this repo proves
nothing about building *against* it — a consumer gets only what `paths` lists
and runs build.zig as a dependency, not as root. Dev-only build steps sit
behind `if (b.pkg_hash.len != 0) return;` for the same reason. CI runs it.

Still open: the milestone-2 in-VPC benchmark numbers. The loopback numbers in
docs/notes.md were re-measured on a quiet machine (three runs; p50 is stable,
p99/p999 are not).

Next up:
- Run `vpc/run.sh` between two machines on wired Ethernet, as a real-network
  check ahead of the VPC run. Not the milestone-2 number. `vpc/README.md` is
  cloud-neutral and covers this case; where the TSDB consumer runs is still
  not recorded, and that decides which placement the milestone-2 run uses.

Inbound size limits are done: frames over `SETTINGS_MAX_FRAME_SIZE` are refused
before their length sizes a buffer, a header block is capped at 64KB both
compressed (CONTINUATION floods) and decoded (HPACK back-reference expansion),
`SETTINGS_MAX_HEADER_LIST_SIZE` is advertised, and a stream carries at most two
HEADERS frames. Each has a unit test in h2.zig that failed first.

Keepalive is done, both sides, as HTTP/2 PING: silent for `interval_ns`, then a
PING, then `error.KeepaliveTimeout` if nothing at all arrives within
`timeout_ns` (any frame counts). Server default 30s/10s, which is what frees the
slot of a vanished client. Client default is 6min/20s, deliberately slow:
grpc-go servers send GOAWAY(ENHANCE_YOUR_CALM) for pings under 5min apart, so
lower it only against zgrpc peers. No `SO_KEEPALIVE` and no idle-connection
timeout — pooled connections are meant to sit idle, and PING already covers the
dead-peer case. A PING write stuck behind a peer that has stopped draining TCP
blocks the task, so that case is still uncovered.

Graceful shutdown is done: cancel the task running `serve`, call
`Server.shutdown(grace_ns)`, then `deinit`. Each connection sends GOAWAY naming
no limit plus a PING, and only when the ack proves the peer has seen it sends
the real last stream id (RFC 9113 6.8, so a stream opened mid-flight is not
refused unretryably). Calls in flight finish within the grace period;
`shutdown` returns false if some outlast it and `deinit` cuts those. Two
behaviours changed to make it work: cancelling `serve` no longer tears down its
connections (only `deinit` and a real accept failure do), and a reader no
longer stops on GOAWAY — it runs until the peer closes, answering PINGs, and
streams we opened past the GOAWAY's last id fail at once. Not checked against
a grpc-go client; interop only covers grpc-go as the other side of unary and
streaming calls.

Request metadata is done, in one direction: `CallOptions.metadata` sends
headers, `CallContext.metadata(name)` reads them, and grpc-go's arrive intact
(interop check). Names are validated (lowercase, nothing the transport owns,
no `grpc-` prefix, no `-bin`) and values must be printable ASCII. Deliberately
not built: response headers and trailers set by a handler, readable by a
client, and binary (`-bin`) metadata. harvezter's CLAUDE.md states no need for
either — one trusted VPC, no auth — so they wait for a stated use.

The smaller items are settled. A request with a `grpc-encoding` other than
identity now gets UNIMPLEMENTED (it was INTERNAL), so a caller can tell the
encoding is at fault; checked against grpc-go with gzip. Compression itself is
still unsupported, and a client receiving a compressed message still gets
`error.CompressionNotSupported`. Unexpected handler errors are now logged
server-side with the method path; callers still see `@errorName(err)` as the
INTERNAL message, which is useful in a trusted VPC and worth revisiting before
anything leaves it. Metrics and logging hooks are not built: they are
interceptor-shaped, which the scope rules exclude, and no consumer has asked.

Rapid reset is measured and handled (numbers in docs/notes.md, tool is
`zig build flood`). Live streams were already bounded at 128 because a reset
stream keeps its slot until its call task ends. The cost was a task, i.e. a
thread, per accepted stream: 139 threads and 46MB under a single attacker, now
10 and 12.6MB, because `acceptStream` drops streams reset while they waited.
What remained was raw CPU, so `Server.Options.reset_limit` bounds the rate:
token bucket per connection, default 2,000/s with a burst of 4,000, over which a
peer gets GOAWAY(ENHANCE_YOUR_CALM) and is disconnected. Four attackers that
took a bystander's p99 from 0.3ms to 7ms are now all cut off within the burst
and the bystander sees 0.35ms. The risk is an honest pool timing out in bulk
during an outage; the defaults are far above ordinary traffic and `null` turns
it off. The client has no such limit — nothing a server does is worth one yet.

Every item the source-reading pass found is now done. What is left is the
milestone-2 in-VPC numbers and the "Next up" item above.
