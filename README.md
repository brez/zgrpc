# zgrpc

[![CI](https://github.com/brez/zgrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/brez/zgrpc/actions/workflows/ci.yml)

Real gRPC over HTTP/2 (h2c) for Zig — the actual wire protocol, verified
against the reference `grpc-go` implementation.

Unary and server-streaming calls, concurrent connections and calls, a
connection pool, enforced deadlines, and typed stubs generated from a `.proto`.
Zig 0.16.0.

## Installing

```
zig fetch --save "git+https://github.com/brez/zgrpc#v0.4.0"
```

```zig
const zgrpc_dep = b.dependency("zgrpc", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("zgrpc", zgrpc_dep.module("zgrpc"));
```

The only dependency is `hendriknielaender/http2.zig`, for its frame codec and
HPACK. There is deliberately no protobuf dependency: this library moves
`[]const u8`, and the optional stub generator ([Protobuf](#protobuf)) fetches
`Arwalk/zig-protobuf` only if you ask for it.

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
works for any codec. Typed handlers are [generated on top](#protobuf).

## Protobuf

zgrpc moves `[]const u8`, and those bytes are the serialized message, so a
`.proto` fits with types from `protoc` and code that decodes and encodes at the
edges. A protoc plugin, `protoc-gen-zgrpc`, writes that code for you: from each
`service` it generates the method paths, a typed client, and the handler table
a server takes. It is opt-in, because it needs
[zig-protobuf](https://github.com/Arwalk/zig-protobuf); leave it off and nothing
protobuf is ever fetched.

```zig
// build.zig
const zgrpc_dep = b.dependency("zgrpc", .{ .target = target, .optimize = optimize, .generator = true });
const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });

const options: protobuf.RunProtocStep.Options = .{
    .destination_directory = b.path("src"),
    .source_files = &.{b.path("proto/helloworld.proto")},
    .include_directories = &.{b.path("proto")},
};
const gen = b.step("gen-proto", "generate message types and stubs");
gen.dependOn(&protobuf.RunProtocStep.create(protobuf_dep.builder, target, options).step); // helloworld.pb.zig
gen.dependOn(&protobuf.RunProtocStep.createWithGenerator( // helloworld.zgrpc.zig
    protobuf_dep.builder,
    zgrpc_dep.artifact("protoc-gen-zgrpc"),
    options,
).step);
```

```zig
const pb = @import("helloworld.pb.zig");
const stubs = @import("helloworld.zgrpc.zig");

// Server: one typed function per rpc. The stub decodes the request (a message
// that does not decode is INVALID_ARGUMENT) and encodes the reply.
const Greeter = struct {
    pub fn sayHello(ctx: *zgrpc.CallContext, request: pb.HelloRequest) anyerror!pb.HelloReply {
        return .{ .message = try std.fmt.allocPrint(ctx.allocator, "hello {s}", .{request.name}) };
    }
    // A server-streaming rpc takes `sender: *zgrpc.typed.Sender(pb.HelloReply)`
    // and calls `try sender.send(reply)` per message.
};
const methods = stubs.Greeter.Methods(Greeter).all; // `++` several services together
var server = try zgrpc.Server.listen(allocator, io, address, &methods, .{ .user_data = &state });
// Handlers reach `state` with ctx.userData(State): they are plain functions.

// Client: typed calls over a Pool.
const greeter = stubs.Greeter.Client.init(pool);
var reply = try greeter.sayHello(allocator, .{ .name = "zig" }, .{});
defer reply.deinit();
if (reply.message) |m| { /* m.message */ } else { /* reply.status.code */ }
```

A server-streaming call returns a receiver: `while (try stream.next()) |m|`, then
`stream.finalStatus()`. Unary and server-streaming rpcs only; a client- or
bidirectionally-streaming one fails generation rather than vanishing from the
stub. A `.proto` needs a `package`, because zig-protobuf writes one file per
package and the stubs import it.

[`examples/protobuf/`](examples/protobuf) runs all of this: `zig build run`, and
`zig build gen-proto` regenerates both files. It is a standalone project because
the generator needs zig-protobuf and zgrpc itself must not.

The calls underneath are `zgrpc.typed`, which works with any message type that
has `encode(writer, allocator)` and `decode(reader, allocator)` in
zig-protobuf's shape, so the plugin is a convenience and not a requirement.
Without it you decode and encode by hand around the byte API, as in the
Server and Client sections above.

## Interceptors

An interceptor wraps every call, on either side: authentication, logging,
metrics, tracing. It works with bytes and the `CallContext`, so one serves every
service, and typed stubs go through it too. Call `next` to continue, or return
without it to answer the call yourself.

```zig
const Auth = struct { token: []const u8 };

fn requireToken(context: ?*anyopaque, ctx: *zgrpc.CallContext, request: []const u8, next: zgrpc.server.UnaryNext) anyerror![]const u8 {
    const auth: *Auth = @ptrCast(@alignCast(context.?));
    const presented = ctx.metadata("authorization") orelse return ctx.fail(.unauthenticated, "no credentials");
    if (!std.mem.eql(u8, presented, auth.token)) return ctx.fail(.permission_denied, "wrong credentials");
    return next.call(ctx, request);
}

var auth: Auth = .{ .token = "secret" };
const chain = [_]zgrpc.ServerInterceptor{.{ .context = &auth, .unary = requireToken }};
var server = try zgrpc.Server.listen(allocator, io, address, &methods, .{ .interceptors = &chain });
```

The client side takes `zgrpc.ClientInterceptor` in `Pool.Options.interceptors`,
and receives `options` to change before passing it to `next.call(request, options)`:
attach `metadata`, tighten the deadline. What comes back is the outcome, a
`Response` with its status or a transport error, so an interceptor can count
statuses or time the call.

The first one listed is outermost, and each kind is optional: set `unary`,
`server_stream`, or both, and a call of the other kind skips it. A
server-streaming interceptor sees the call begin and end but not each message.
On the server they run only for a method that exists and a request that reached
a handler, so an unknown path is answered `UNIMPLEMENTED` before any of them. An
empty list costs a call a slice-length check.

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
  rapid-reset attack. `null` turns it off. It is per connection, and callers
  timing out in bulk during an outage reset streams about as fast as they were
  calling, so keep it well above your peak call rate divided by the pool size
  (default 4), or an honest pool gets cut off when it can least afford it.
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
- **No retries, load balancing or name resolution.** A pool is several
  connections to *one* peer.
- **Stubs are unary and server-streaming only**, and carry from the .proto its
  names, types and the comments above a service and its rpcs, but no options or
  deprecation. See [Protobuf](#protobuf).
- **Deadlines bound waiting, not working.** Every wait a call makes is bounded,
  but a handler burning CPU cannot be preempted, and neither connecting nor a
  socket write to a peer that has stopped draining TCP is covered.
- **Flow-control windows are fixed**, at 256KB per stream and 1MB per
  connection, rather than adapting to the bandwidth-delay product.

## Development

```
zig build test              # unit + integration tests (129)
zig build test-unit         # pure unit tests only, no sockets
zig build test-integration  # real client/server over loopback
zig build bench             # loopback latency, plus pool throughput under load
zig build test-gen -Dgenerator=true  # the stub generator's tests (6)
./interop/run.sh            # grpc-go interop suite, 15 checks (requires Go)
./packaging/verify.sh       # fetch and use it as a published dependency

cd examples/protobuf && zig build run        # the protobuf example, on generated stubs
cd examples/protobuf && zig build gen-proto  # regenerate its .pb.zig and .zgrpc.zig
```

`interop/` is the check that matters: our own client agreeing with our own
server proves only self-consistency. `packaging/verify.sh` is the other one —
building in this repo does not prove a consumer can build against it.

[docs/notes.md](docs/notes.md) has the design rationale and the measurements
behind it: what the concurrency model costs, why the flow-control windows are
the size they are, the bugs interop and benchmarking found, and the in-VPC
latency numbers (us-east-1: worst fixed-rate p999 under 1ms against a ~5ms
budget).

## Versioning

**v0.4.0**, semantic, with 0.x meaning the Zig API may still move — with one
consumer so far, an awkward API is worth breaking rather than living with. The wire
behaviour is the stable part, and the interop suite is what a bump has to keep
passing.

## License

MIT — see [LICENSE](LICENSE).
