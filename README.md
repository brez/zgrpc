# zgrpc

[![CI](https://github.com/brez/zgrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/brez/zgrpc/actions/workflows/ci.yml)

Real gRPC over HTTP/2 (h2c) for Zig — actual wire protocol, verified against
the reference `grpc-go` implementation.

Status: **unary and server streaming work end to end** and interoperate with
grpc-go (milestones 1 and 3).

```
zig build test              # unit + integration tests (72)
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
try server.acceptAndServe();
```

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

Current numbers (Raspberry Pi 5, loopback, ReleaseFast):

```
     64 B  n=10000   mean    18.6us  p50    18.5us  p99   21.1us  p999    32.5us
   1024 B  n=10000   mean    19.8us  p50    19.5us  p99   29.3us  p999    33.1us
  16384 B  n=10000   mean    41.7us  p50    44.6us  p99   71.9us  p999   273.4us
 131072 B  n= 2000   mean   342.8us  p50   318.1us  p99  658.1us  p999  1379.5us
```

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

## Known constraints

**Single-threaded connection handling.** `Hpack.decodeHeaderFieldView` in the
dependency calls `ScratchBuffer.reset()` on entry and returns slices that may
point into that **process-wide** buffer. Two threads decoding header blocks
concurrently would corrupt each other. Consequences:

1. `src/h2.zig` copies every header into an arena immediately after decoding
   it, before decoding the next — a lazy copy would silently corrupt all but
   the last header.
2. The server serves connections sequentially. Thread-per-connection needs
   either a per-connection decoder upstream or a lock around decoding. This is
   the first thing to fix before the milestone-2 load benchmark, since it caps
   server throughput at one connection.

**Stream cancellation is bounded by flow control, not immediate.** A client
that abandons a stream sends RST_STREAM and stops replenishing that stream's
flow-control window; the server notices when its write path next blocks for
credit, and `stream.send` then returns `error.StreamReset`, unwinding the
handler. This works, but detection is only as prompt as the window is small: a
peer that advertises large or dynamically-grown windows (grpc-go does) can let
a server produce well past the point of cancellation before it blocks and
looks.

Making it immediate needs the transport to read frames while it is writing,
which this single-threaded design cannot do. A non-blocking `poll` before each
send was tried and removed: `std.Io.net.Stream.Reader` latches its error state,
so one failed `fillMore` poisons the reader and every later read fails with
`error.ReadFailed`. The real fix is a reader that is not driven by the writer —
the same restructuring the concurrency limit above needs.

**No compression.** The compressed-message flag is parsed and rejected;
`grpc-encoding` is not negotiated.

**Flow control is connection- and stream-level, but there is no stream
concurrency limit.** `SETTINGS_MAX_CONCURRENT_STREAMS` from the peer is parsed
and ignored, which is safe only because the client issues one call at a time.

**h2c only, no TLS.** Per the project constraint that all consumers are inside
one trusted VPC. There is no ALPN, so this cannot talk to a TLS gRPC endpoint.

## Interop

`./interop/run.sh` runs the reference `grpc-go` client against the Zig server.
This is the check that matters for "must be real gRPC on the wire" — our own
client agreeing with our own server proves only self-consistency. It covers
the unary happy path, empty messages, messages spanning many DATA frames and
exceeding the flow-control window, non-OK status codes with messages, unknown
methods, connection reuse across calls, and server streaming — ordered
delivery, empty streams, long streams, and a mid-stream failure whose status
must arrive in trailers after the messages already sent.

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

## Layout

```
src/h2.zig               HTTP/2 connection driver (frames + HPACK from http2.zig)
src/framing.zig          gRPC 5-byte length-prefixed message framing
src/status.zig           grpc-status codes, percent-encoded grpc-message
src/timeout.zig          grpc-timeout deadline encoding
src/client.zig           unary + server-streaming client
src/server.zig           unary + server-streaming server
src/tcp.zig              TCP_NODELAY
src/integration_test.zig end-to-end tests over loopback
examples/echo_server.zig standalone server (interop target)
examples/bench.zig       loopback latency baseline
interop/                 grpc-go interop suite
```

## License

MIT — see [LICENSE](LICENSE).
