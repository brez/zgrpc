# zgrpc

[![CI](https://github.com/brez/zgrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/brez/zgrpc/actions/workflows/ci.yml)

Real gRPC over HTTP/2 (h2c) for Zig — the actual wire protocol, verified
against the reference `grpc-go` implementation.

Unary and server-streaming calls, concurrent connections and calls, a
connection pool, and enforced deadlines. Zig 0.16.0.

## Installing

```
zig fetch --save "git+https://github.com/brez/zgrpc#v0.1.0"
```

```zig
const zgrpc_dep = b.dependency("zgrpc", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("zgrpc", zgrpc_dep.module("zgrpc"));
```

The only dependency is `hendriknielaender/http2.zig`, for its frame codec and
HPACK. There is deliberately no protobuf dependency: this library moves
`[]const u8` and codegen sits above it, so depend on `Arwalk/zig-protobuf`
yourself rather than inheriting a pin from here.

## Server

```zig
fn sayHello(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    // `request` is the serialized message. Anything allocated from
    // ctx.allocator is freed when the call ends.
    if (request.len == 0) return ctx.fail(.invalid_argument, "empty request");
    return ctx.allocator.dupe(u8, request);
}

fn rangeQuery(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    var rows = try openCursor(ctx.allocator, request);
    while (try rows.next()) |row| {
        // Goes out immediately; nothing accumulates a full response. Fails
        // with error.StreamReset or error.DeadlineExceeded once the call is
        // over, which unwinds the handler and stops the query.
        try stream.send(row);
    }
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary("/helloworld.Greeter/SayHello", sayHello),
    zgrpc.Method.serverStream("/tsdb.Query/Range", rangeQuery),
};

var server = try zgrpc.Server.listen(allocator, io, address, &methods, .{});
defer server.deinit();
try server.serve();
```

`serve` accepts until cancelled, and gives every connection and every call its
own unit of concurrency.

## Client

```zig
const pool = try zgrpc.Pool.init(allocator, io, address, "host:50051", .{});
defer pool.deinit();

var response = pool.call("/helloworld.Greeter/SayHello", request_bytes, .{
    .deadline_ns = 5 * std.time.ns_per_ms,
}) catch |err| switch (err) {
    error.DeadlineExceeded => return, // this client's clock ran out
    else => return err,
};
defer response.deinit();

if (!response.call_status.isOk()) { /* .code, .message */ }
```

```zig
var stream = try pool.callServerStream("/tsdb.Query/Range", request, .{});
defer stream.deinit();

while (try stream.next()) |message| {
    // Borrows the decoder buffer; valid until the next `next`. Copy to retain
    // — that is what keeps streaming from allocating per message.
}
if (!stream.finalStatus().isOk()) { /* stream.finalStatus().code */ }
```

`Pool` holds several connections to one peer and replaces any the peer closes.
A new one is opened only when every existing one is busy, so `Options.size` is
a ceiling rather than a target and a caller making one call at a time holds one
connection. `zgrpc.Client` is the single connection underneath, if you want it
directly.

Handlers take and return **bytes**, not generated types, so the same shape
works for any codec.

## What it does not do

- **No TLS.** h2c only, and no ALPN, so it cannot talk to a TLS endpoint.
- **No compression.** The compressed-message flag is parsed and rejected.
- **No retries, load balancing, interceptors or name resolution.** A pool is
  several connections to *one* peer.
- **Deadlines bound waiting, not working.** Every wait a call makes is bounded,
  but a handler burning CPU cannot be preempted, and neither connecting nor a
  socket write to a peer that has stopped draining TCP is covered.
- **Flow-control windows are fixed**, at 256KB per stream and 1MB per
  connection, rather than adapting to the bandwidth-delay product.

## Development

```
zig build test              # unit + integration tests (81)
zig build test-unit         # pure unit tests only, no sockets
zig build test-integration  # real client/server over loopback
zig build bench             # loopback latency, plus pool throughput under load
./interop/run.sh            # grpc-go interop suite, 13 checks (requires Go)
./packaging/verify.sh       # fetch and use it as a published dependency
```

`interop/` is the check that matters: our own client agreeing with our own
server proves only self-consistency. `packaging/verify.sh` is the other one —
building in this repo does not prove a consumer can build against it.

[docs/notes.md](docs/notes.md) has the design rationale and the measurements
behind it: what the concurrency model costs, why the flow-control windows are
the size they are, and the bugs interop and benchmarking found.

## Versioning

**v0.1.0**, semantic, with 0.x meaning the Zig API may still move — with one
consumer, an awkward API is worth breaking rather than living with. The wire
behaviour is the stable part, and the interop suite is what a bump has to keep
passing.

## License

MIT — see [LICENSE](LICENSE).
