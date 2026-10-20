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
- No retries, no client-side load balancing, no service discovery. These are
  real gRPC features but nobody needs them yet. (Interceptors were on this
  list and are built; see the exception below.)
- No TLS/ALPN until a concrete use case requires leaving the VPC.

One deliberate exception: on 2026-10-19 the stated goal became a general-purpose
library, not one shaped around harvezter, so stubs and then interceptors were
built ahead of a second consumer, and retries, name resolution and load
balancing are planned, in that order (see "Next up"). The rule above still
governs everything else on that list.

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
     zgrpc's handler shape, so nothing plugs in. A protoc plugin emitting zgrpc
     handlers was the fix, and `gen/` is it (see "Decisions made", Stubs).

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

- ~~Which HTTP/2 library wins the milestone-2 benchmark?~~ Answered:
  `http2.zig` stays. us-east-1 c7g.large numbers (2026-10-19) are in
  docs/notes.md: worst fixed-rate p999 under 1ms against a ~5ms budget, no
  tail problem traced to the library.
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
- `zig build test-gen -Dgenerator=true` — the stub generator's tests (fetches
  zig-protobuf; the option is what makes the dependency non-lazy)
- `cd examples/protobuf && zig build run` — protobuf and generated stubs end to
  end; a standalone project so that zgrpc itself keeps having no protobuf
  dependency. `zig build gen-proto` there regenerates `src/helloworld.*.zig`
  (CI fails if the committed copies are stale)

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
the library module has **no protobuf dependency**. (v0.1.0 had one wired into
every module and imported nowhere, which is why it was removed.) A consumer
that wants generated types depends on `Arwalk/zig-protobuf` directly rather
than inheriting our pin; the one exception is the opt-in generator below. The
pinning notes above still apply, which is why they stay here.

**Stubs (`gen/`, `src/typed.zig`)**: unary and server-streaming only, matching
what the library speaks. Three decisions, each forced by something:
- *The typed layer is in zgrpc, the generator is not.* `zgrpc.typed` is
  duck-typed over `encode(writer, allocator)` / `decode(reader, allocator)` —
  zig-protobuf's shape, imported from nowhere — so it is testable with a fake
  message type and the library still needs no protobuf. The generated files
  are then only names, which is why they are small. A message that does not
  decode is INVALID_ARGUMENT, not INTERNAL.
- *The generator ships inside this package behind `-Dgenerator=true`, with
  zig-protobuf as a **lazy** dependency.* A plugin must parse protoc's
  descriptors, so it needs zig-protobuf; and Zig cannot depend on a
  subdirectory of a repo, so a separate `gen/` package could not be fetched.
  Lazy means a project that does not pass the option never fetches it —
  checked against an empty package cache, not assumed. `gen/google/*.pb.zig`
  are zig-protobuf's own bootstrapped descriptor types, copied because it does
  not export them (`gen/NOTICE`).
- *Handlers needed a context pointer.* They are bare function pointers, so a
  typed handler could reach no state but globals. `Server.Options.user_data`
  surfaces as `ctx.userData(T)`; additive, null by default.

Output is one `<package>.zgrpc.zig` per package, next to zig-protobuf's
`<package>.pb.zig`, because that is how its files are laid out. It runs as a
second protoc pass through `RunProtocStep.createWithGenerator`, which only
hands protoc a plugin and an output directory. A client- or
bidirectionally-streaming rpc fails generation: a stub missing a method is a
worse surprise than no stub. The comment above a service or an rpc becomes a
doc comment (client methods, service); options and deprecation are not carried.
Not checked against grpc-go: the typed layer changes only what turns bytes
into messages, and interop already covers the bytes.

**Interceptors** (`server.Interceptor`, `pool.Interceptor`): a function pointer
plus a context pointer, since Zig has no closures, chained by recursive `Next`
structs over a borrowed slice, so a call allocates nothing and an empty list
costs a slice-length check. Bytes-level on purpose, for the same reason
handlers are: one interceptor serves every service, and the typed stubs sit
below them. Two non-obvious limits, both documented: a streaming interceptor
sees the call begin and end, not each message (that needs a wrapping stream
type, which nobody has asked for), and on the server they do not run for an
unknown method or a request refused before a handler (UNIMPLEMENTED is
answered first), which an auth interceptor's author should know. Client
interceptors live on `Pool`, not `Client`: the pool is the top-level call API,
and the later `Channel` will take the same chain. Interceptor functions return
`anyerror`, which widens `Pool.call`'s error set; callers already needed an
`else` for transport errors.

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

Milestone 2 is measured: same-AZ and cross-AZ on c7g.large in us-east-1, numbers
in docs/notes.md. The loopback numbers there were re-measured on a quiet
machine (three runs; p50 is stable, p99/p999 are not).

Next up:
- Milestone 2 is a reference measurement, not a gate on any consumer: the
  2026-10-19 run is one machine type (c7g.large) in one region (us-east-1, 1a-1a
  and 1a-1c; c7g.large is not offered in 1b). Anyone with different hardware
  or placement reruns `vpc/run.sh` and compares; no consumer's layout is
  waited on.
- `reset_limit` is a library default (2,000/s per connection, burst 4,000),
  untested against a real outage pattern. The README says how to size it
  (above peak call rate divided by pool size); raise it for a busier pool, not
  `null`, which removes the only protection against a client resetting in a
  loop.
- Retries, next. Decided: retry a call the server provably never started
  (refused stream, GOAWAY past the last id) always; retry UNAVAILABLE and the
  like only when the caller marks the call idempotent, because a retried write
  that did reach the server can run twice. Per-call policy with attempts,
  exponential backoff with jitter, bounded by the call's deadline; streams only
  before the first message. Lives in the pool, not as an interceptor, since it
  needs to know about connections.
- Then a `Channel`: a resolver (DNS or a static list, re-resolved on a
  schedule) feeding one `Pool` per address, a picker over them, a down-mark with
  backoff for a failing address, and retries gaining the option of another
  peer. `zgrpc.typed` already needs only `call`/`callServerStream`, so stubs
  would take a `Channel` as readily as a `Pool`.
- Let harvezter use the stubs (released in v0.3.0): bump its pin, and adopt the
  generated client and `Methods(Impl)` once it has a `.proto` of its own.
- The wired-Ethernet two-host check is no longer needed ahead of anything.

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
anything leaves it. No metrics or logging hooks ship: they are interceptors
now, so a consumer writes the one it wants (the README shows the shape).

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
"Next up" item above.
