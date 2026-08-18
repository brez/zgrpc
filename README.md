# zgrpc

[![CI](https://github.com/brez/zgrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/brez/zgrpc/actions/workflows/ci.yml)

Real gRPC over HTTP/2 (h2c) for Zig — actual wire protocol, verified against
the reference `grpc-go` implementation.

Status: **unary and server streaming work end to end**, concurrently, and
interoperate with grpc-go (milestones 1 and 3).

```
zig build test              # unit + integration tests (75)
zig build test-unit         # pure unit tests only, no sockets
zig build test-integration  # real client/server over loopback
zig build bench             # loopback latency baseline
zig build bench-build       # compile the benchmark without running it
zig build test-integration-build   # compile integration tests without running
./interop/run.sh            # grpc-go interop suite (requires Go)
```

## Using it

Server:

```zig
fn sayHello(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    // `request` is the serialized protobuf message; decode it with
    // zig-protobuf. Anything allocated from ctx.allocator is freed when the
    // call ends.
    if (request.len == 0) return ctx.fail(.invalid_argument, "empty request");
    return ctx.allocator.dupe(u8, request);
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary("/helloworld.Greeter/SayHello", sayHello),
};

var server = try zgrpc.Server.listen(allocator, io, address, &methods, .{});
defer server.deinit();
try server.serve();
```

`serve` accepts until cancelled and gives each connection its own unit of
concurrency, and each call on it another. `acceptAndServe` handles a single
connection on the calling task, for callers that want to drive the accept loop
themselves. See "Concurrency" below.

Client:

```zig
const client = try zgrpc.Client.connect(allocator, io, address, "host:50051", .{});
defer client.deinit();

var response = try client.call("/helloworld.Greeter/SayHello", request_bytes, .{
    .deadline_ns = 5 * std.time.ns_per_ms,
});
defer response.deinit();

if (!response.call_status.isOk()) { /* response.call_status.code / .message */ }
```

Server streaming:

```zig
fn rangeQuery(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    var rows = try openCursor(ctx.allocator, request);
    while (try rows.next()) |row| {
        // Each send goes out immediately; nothing accumulates a full response.
        // Returns error.StreamReset once the client cancels, which unwinds the
        // handler and stops the query.
        try stream.send(row);
    }
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.serverStream("/tsdb.Query/Range", rangeQuery),
};
```

Consuming one:

```zig
var stream = try client.callServerStream("/tsdb.Query/Range", request, .{});
defer stream.deinit();

while (try stream.next()) |message| {
    // Borrows the decoder buffer; valid until the next `next` call. Copy to
    // retain. This is what keeps streaming from allocating per message.
}
if (!stream.finalStatus().isOk()) { /* stream.finalStatus().code */ }
```

Handlers take and return **bytes**, not generated types. Protobuf codegen
belongs above this layer, so the transport never needs to know about message
types — and the same handler shape works for any codec.

## The HTTP/2 decision

CLAUDE.md posed this as the most consequential choice in the project. Both
candidates were evaluated on source, not docs.

**Chosen: `hendriknielaender/http2.zig`, but only its frame codec and HPACK.**
The gRPC stream sequence is driven directly by `src/h2.zig`.

Neither library's high-level API can express gRPC, and the reason is
structural rather than a missing feature. A unary gRPC response is:

```
HEADERS (:status, content-type)      -- no END_STREAM
DATA    (length-prefixed message)    -- no END_STREAM
HEADERS (grpc-status, grpc-message)  -- END_STREAM   <- trailers
```

`grpc-status` lives in that *trailing* HEADERS frame, after the body. But
`http2.zig`'s connection state machine sends response headers at most once
(`if (stream.response_headers_sent) return;` in `connection.zig`) and attaches
END_STREAM to the final DATA frame, so a second header block is unreachable
through its request/response API.

| | `http2.zig` | `httpx.zig` |
|---|---|---|
| HTTP/2 trailers | validation only, not sendable | none — its trailer code is HTTP/1.1 chunked |
| API shape | request/response state machine | route table (`server.get(path, fn)`) |
| Frame + HPACK primitives | public and usable standalone | not separable from the server |
| Client mode | yes (`Connection.init(..., is_server)`) | high-level client only |
| Built on | `*std.Io.Reader` / `*std.Io.Writer` | own abstractions |
| Last commit | 2026-05-01 | 2026-08-08 |
| Scope | HTTP/2 only (~18k LOC) | HTTP/1+2+3, WebSocket, proxy (~36k LOC) |

`http2.zig` won because the parts actually needed — `Frame`, `FrameHeader`,
`Hpack` — are public, standalone, and built directly on Zig 0.16's
`std.Io.Reader`/`Writer`, which is also what zig-protobuf's `StreamDecoder`
consumes. `httpx.zig` is more actively maintained and does h2c fine, but its
HTTP/2 is only reachable through a route-table server API with no trailer
path at any layer.

This keeps the "do not hand-roll HTTP/2" constraint where it matters: HPACK,
Huffman coding, and frame parsing are still the dependency's. What `src/h2.zig`
owns is the connection lifecycle and stream sequencing — the part that has to
be gRPC-shaped.

Note the dependency's default `zig build` fails, because its TLS targets need
BoringSSL. This does not affect us: the `http2` module is declared separately
from the TLS modules and pulls in no BoringSSL, which is verified by the fact
that this project builds at all.

## Latency

`zig build bench` measures unary calls over loopback, so it reports the
library's own cost with network RTT removed. It is a floor, not a prediction:
the milestone-2 number has to come from the real VPC, where same-AZ vs
cross-AZ RTT dominates the ~5ms budget.

Reference numbers (Raspberry Pi 5, loopback, ReleaseFast, idle machine), taken
on the single-task-per-connection transport that preceded the concurrent one:

```
     64 B  n=10000   mean    18.6us  p50    18.5us  p99   21.1us  p999    32.5us
   1024 B  n=10000   mean    19.8us  p50    19.5us  p99   29.3us  p999    33.1us
  16384 B  n=10000   mean    41.7us  p50    44.6us  p99   71.9us  p999   273.4us
 131072 B  n= 2000   mean   342.8us  p50   318.1us  p99  658.1us  p999  1379.5us
```

### What concurrency cost

The dedicated reader task (see "Concurrency") puts a thread handoff in the
middle of every call: the reply is parsed by the connection's reader and the
caller is woken to collect it. That is not free, and it was measured rather
than assumed — old transport and new, built from the same tree and run
back to back:

```
                 p50 before   p50 after
     64 B            17.8us      29.0us
   1024 B            18.2us      31.0us
  16384 B            45.0us      55.0us
 131072 B           360.0us     530.0us
```

About 11µs of that is one context switch, and it is the price of the whole
feature: concurrent calls and cancellation that does not wait for a window to
close. Against a ~5ms in-VPC budget it is 0.2%. The 128KB gap is larger
because a multi-frame message crosses the handoff several times.

Two things were found chasing it, both worth more than the handoff itself:

* **Wakeups were being spent three-to-a-call.** A unary response is HEADERS,
  DATA and trailers, written with one flush and therefore read in one go — but
  the reader woke the caller after each frame. Deferring wakeups until the
  reader is actually about to block on the socket collapses them into one, and
  took the 64-byte p99 from ~610µs to ~230µs.
* **The per-stream receive buffer compacted on every read**, making a message
  that spans many frames quadratic in its own size. Consumed space is now
  reclaimed by the append that needs the room.

These numbers are an A/B, not a result: the machine had a browser on it and
its load shows in every tail figure — the *old* code measured p999 609µs to
2.1ms on the same runs, against the 32.5µs in the table above. The table stands
as the last clean measurement; a clean re-measurement on an idle machine is
owed before any of it is quoted.

Getting there found a real bug worth recording, because it is exactly the kind
of tail problem the project brief warns about — and it was *not* the HTTP/2
library:

```
before TCP_NODELAY:  64 B  p50    95.5us  p99 2500.3us  p999 48026.8us
after  TCP_NODELAY:  64 B  p50    18.5us  p99   21.1us  p999    32.5us
```

A p50 of 95µs beside a p999 of 48ms — a 1,478x tail — is the signature of
Nagle's algorithm meeting the peer's delayed ACK. gRPC is a ping-pong of small
writes, which is precisely the pattern where the sender withholds a small
segment awaiting an ACK the receiver is withholding awaiting more data; on
Linux the standoff breaks only when the 40ms delayed-ACK timer fires.
`std.Io.net` does not set TCP_NODELAY, so `src/tcp.zig` sets it on both
accepted and dialled sockets.

Two lessons for milestone 2: percentiles were essential (the mean and p50 both
looked fine while one call in a thousand took 48ms), and a bad tail is worth
attributing before optimising — the brief's guess that a bad tail would
implicate the HTTP/2 library would have sent the investigation the wrong way
here.

The benchmark also deliberately avoids `DebugAllocator`, which captures a stack
trace per allocation and on ARM costs more than the RPC being measured.

### Flow-control windows

We advertise a 256KB stream window and raise the connection window to 1MB,
rather than living with HTTP/2's 65,535-byte default. The default is a latency
bug at any real bandwidth: a sender may have only that much in flight before it
must stop and wait for credit, so once bandwidth times round-trip exceeds it,
every message larger than the window costs an extra round trip. In-VPC at
10Gbps the bandwidth-delay product is around half a megabyte — the default
misses it by an order of magnitude.

The connection window cannot be set in SETTINGS; RFC 9113 6.9.2 fixes its
initial value and only a WINDOW_UPDATE on stream 0 moves it, so that frame goes
out alongside our SETTINGS.

The cost is memory, and it is deliberate: the connection window is exactly the
bound on what a peer can make one connection buffer before we have consumed any
of it. Multiplied by `Server.Options.max_concurrent_connections`, it is the
server's worst case. Credit is returned when a caller *consumes* bytes, not
when they arrive — returning it on arrival, as an earlier version did, means
there is no bound at all.

## Concurrency

Three levels, each on its own unit of concurrency:

* **A connection per accept.** `serve` spawns each accepted connection and
  keeps accepting. Bounded by `Options.max_concurrent_connections`.
* **A reader per connection.** One task owns the read side and does nothing
  else: it pulls frames off the socket, decodes header blocks, and files
  everything into per-stream mailboxes. Callers never read from the socket.
* **A call per stream.** Each stream the peer opens is handled on its own task,
  so a handler that is slow, blocked, or streaming a million rows holds up
  nothing but itself. Clients get the same thing from the other side: `call`
  may be invoked from several tasks on one `Client`, and the calls multiplex.

Writers take a separate lock from connection state and the two are never held
at once, so a writer blocked in `flush` cannot stall frame processing — which
is what would deadlock against a peer that is itself blocked writing to us.

A dedicated reader is what makes cancellation prompt. When the writer drove the
reader, a RST_STREAM sat unread until the writer next happened to block for
flow-control credit, so a server noticed a cancelled call only as promptly as
the peer's window was small — and grpc-go grows windows dynamically. Now the
frame is seen on arrival and the next `stream.send` fails, whatever the window
is doing.

The one rule the structure exists to keep is that a connection's header blocks
are decoded on exactly one task. `Hpack.decodeHeaderFieldView` in the
dependency calls `ScratchBuffer.reset()` on entry and returns slices into that
buffer, which is `threadlocal`; its dynamic table is connection-wide ordered
state. So different threads decoding concurrently are fine, and two of one
connection's blocks interleaved on a single thread are not. Decoding lives in
the reader task alone, which settles it — and `src/h2.zig` still copies each
header into an arena before decoding the next, because a lazy copy would
corrupt all but the last.

## Known constraints

**No compression.** The compressed-message flag is parsed and rejected;
`grpc-encoding` is not negotiated.

**Flow-control windows are fixed, not adaptive.** 256KB per stream and 1MB per
connection, chosen for an in-VPC round trip and never adjusted. grpc-go
estimates the bandwidth-delay product and grows its windows to match; a link
much faster or much slower than assumed will not get the same treatment here.

**No deadline enforcement on the server.** `grpc-timeout` is decoded and handed
to the handler as `ctx.deadline_ns`, but nothing cancels a handler that runs
past it. Handlers that care must check it themselves.

**h2c only, no TLS.** Per the project constraint that all consumers are inside
one trusted VPC. There is no ALPN, so this cannot talk to a TLS gRPC endpoint.

## Interop

`./interop/run.sh` runs the reference `grpc-go` client against the Zig server.
This is the check that matters for "must be real gRPC on the wire" — our own
client agreeing with our own server proves only self-consistency. It covers
the unary happy path, empty messages, messages spanning many DATA frames and
exceeding the flow-control window, non-OK status codes with messages, unknown
methods, connection reuse across calls, server streaming — ordered delivery,
empty streams, long streams, and a mid-stream failure whose status must arrive
in trailers after the messages already sent — and eight concurrent RPCs
multiplexed over the one connection grpc-go keeps open, timed to prove they
overlap rather than queue.

Bugs found this way that the internal tests missed:

- Stream-level `WINDOW_UPDATE` frames were ignored entirely; only the
  connection window was tracked.
- A sender blocked on a closed window consumed the very `WINDOW_UPDATE` it was
  waiting for and kept blocking. Any message over ~32KB deadlocked.
- The flow-control wait loop discarded every frame it read, so a request opened
  while a long response was still being written was silently lost. Frames that
  do not belong to the writer are now stashed for the next reader.

Adding server streaming surfaced one more, caught by an internal test rather
than interop: skipping a HEADERS frame for a stream we were not reading
desynchronised HPACK. The dynamic table is connection-wide state built by every
header block in order, so a skipped block corrupts every later one. Header
blocks are now always decoded, even when discarded.

Making the transport concurrent surfaced two of its own, both found by
benchmarking rather than by a test — see "What concurrency cost": wakeups spent
three to a call, and a per-stream receive buffer that compacted on every read
and so was quadratic in message size.

## Layout

```
src/h2.zig               HTTP/2 connection driver: reader task, per-stream
                         mailboxes, flow control (frames + HPACK from http2.zig)
src/framing.zig          gRPC 5-byte length-prefixed message framing
src/status.zig           grpc-status codes, percent-encoded grpc-message
src/timeout.zig          grpc-timeout deadline encoding
src/client.zig           unary + server-streaming client, concurrent calls
src/server.zig           unary + server-streaming server, task per call
src/tcp.zig              TCP_NODELAY
src/integration_test.zig end-to-end tests over loopback
examples/echo_server.zig standalone server (interop target)
examples/bench.zig       loopback latency baseline
interop/                 grpc-go interop suite
```

## License

MIT — see [LICENSE](LICENSE).
