# zgrpc

[![CI](https://github.com/brez/zgrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/brez/zgrpc/actions/workflows/ci.yml)

Real gRPC over HTTP/2 (h2c) for Zig — the actual wire protocol, verified
against the reference `grpc-go` implementation.

Unary and server-streaming calls, concurrent connections and calls, a
connection pool, and enforced deadlines. Zig 0.16.0.

## Installing

```
zig fetch --save "git+https://github.com/brez/zgrpc#v0.2.0"
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

## Protobuf

zgrpc moves `[]const u8`, and those bytes are the serialized message — so a
`.proto` fits by generating Zig types with `protoc`, decoding at the top of the
handler and encoding at the bottom. Add
[zig-protobuf](https://github.com/Arwalk/zig-protobuf) yourself; codegen is a
build step, and it downloads `protoc` for you.

[`examples/protobuf/`](examples/protobuf) is all of this working: a `.proto`,
the generated types, a server and a client. It is a standalone project because
zgrpc has no protobuf dependency and the example needs one — which is also what
keeps that claim honest. `cd examples/protobuf && zig build run`.

```zig
// build.zig
const protobuf = @import("protobuf");

const gen = b.step("gen-proto", "generate Zig types from .proto");
gen.dependOn(&protobuf.RunProtocStep.create(protobuf_dep.builder, target, .{
    .destination_directory = b.path("src/proto"),
    .source_files = &.{b.path("proto/helloworld.proto")},
    // Must contain the .proto files; protoc rejects paths it cannot see below
    // one of these.
    .include_directories = &.{b.path("proto")},
}).step);
```

```zig
const pb = @import("proto/helloworld.pb.zig");

fn sayHello(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    var reader = std.Io.Reader.fixed(request);
    const req = try pb.HelloRequest.decode(&reader, ctx.allocator);

    const reply: pb.HelloReply = .{
        .message = try std.fmt.allocPrint(ctx.allocator, "hello {s}", .{req.name}),
    };

    var out: std.Io.Writer.Allocating = .init(ctx.allocator);
    try reply.encode(&out.writer, ctx.allocator);
    return out.written();
}
```

Both allocate from `ctx.allocator`, so neither needs cleanup — the arena goes
when the call does. Clients are the mirror image: encode the request, pass the
bytes to `call`, decode `response.message`.

**There is no stub generation.** You write the `/package.Service/Method` path
yourself and pair it with the right request and response types by hand; nothing
checks that you got it right. zig-protobuf does emit a vtable type per
`service` — with `package` and `service_name` constants you can build the path
from — but it is transport-agnostic by design (its README: "service interfaces
only ... does not include a gRPC transport layer") and its handler signatures
are not zgrpc's, so it is not something to plug in. Closing this properly means
a protoc plugin that emits zgrpc handlers, which does not exist yet.

## Operating a server

- **Shutdown.** Cancel the task running `serve`, call `server.shutdown(grace_ns)`,
  then `deinit`. Clients get GOAWAY and calls in flight finish within the grace
  period; `shutdown` returns false if some outlast it, and `deinit` cuts those.
- **Keepalive.** Both ends PING a peer that has been silent and drop it if
  nothing at all arrives in time, so a vanished client frees its connection
  slot. Server default 30s/10s; client default 6min/20s, because grpc-go servers
  answer pings under 5 minutes apart with GOAWAY. `Options.keepalive = null`
  turns it off.
- **Reset limit.** `Server.Options.reset_limit` (default 2,000/s, burst 4,000)
  disconnects a peer that cancels streams faster than that, which is the
  rapid-reset attack. `null` turns it off.
- **Metadata.** `CallOptions.metadata` sends request headers, validated before
  anything goes out; handlers read them with `ctx.metadata("name")`.
- **Limits on what a peer can send.** Frames, header blocks and header lists
  are bounded (64KB for a header block), and a stream carries at most two
  HEADERS frames.

## What it does not do

- **No TLS.** h2c only, and no ALPN, so it cannot talk to a TLS endpoint.
- **No compression.** A request in any encoding but identity gets UNIMPLEMENTED,
  and a compressed response is an error on the client.
- **No response metadata, no binary metadata.** A handler cannot set response
  headers or trailers beyond the status, and `-bin` names are refused.
- **No retries, load balancing, interceptors or name resolution.** A pool is
  several connections to *one* peer.
- **No generated service stubs.** Message types come from protoc; method paths
  and their types are hand-written. See [Protobuf](#protobuf).
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

cd examples/protobuf && zig build run        # the protobuf example
cd examples/protobuf && zig build gen-proto  # regenerate its .pb.zig
```

`interop/` is the check that matters: our own client agreeing with our own
server proves only self-consistency. `packaging/verify.sh` is the other one —
building in this repo does not prove a consumer can build against it.

[docs/notes.md](docs/notes.md) has the design rationale and the measurements
behind it: what the concurrency model costs, why the flow-control windows are
the size they are, and the bugs interop and benchmarking found.

## Versioning

**v0.2.0**, semantic, with 0.x meaning the Zig API may still move — with one
consumer, an awkward API is worth breaking rather than living with. The wire
behaviour is the stable part, and the interop suite is what a bump has to keep
passing.

## License

MIT — see [LICENSE](LICENSE).
