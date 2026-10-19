//! zgrpc with protobuf messages and generated stubs, end to end over a real
//! socket.
//!
//! A standalone project rather than part of the main build, and that is the
//! point: zgrpc has no protobuf dependency, so anything demonstrating protobuf
//! has to bring its own. Keeping it out here is what makes that true rather
//! than merely claimed — see the repository README, "Protobuf".
//!
//!     zig build run        the round trip below
//!     zig build gen-proto  regenerate src/helloworld.*.zig from proto/

const std = @import("std");
const zgrpc = @import("zgrpc");
const pb = @import("helloworld.pb.zig");
const stubs = @import("helloworld.zgrpc.zig");

/// The server side of `service Greeter`: one function per method, typed. The
/// stub decodes the request, encodes the reply, and answers a request that
/// does not decode with INVALID_ARGUMENT, so none of that is written here.
/// Anything allocated from `ctx.allocator` is freed when the call ends.
const Greeter = struct {
    pub fn sayHello(ctx: *zgrpc.CallContext, request: pb.HelloRequest) anyerror!pb.HelloReply {
        if (request.name.len == 0) return ctx.fail(.invalid_argument, "name is required");
        return .{ .message = try std.fmt.allocPrint(ctx.allocator, "hello {s}", .{request.name}) };
    }

    /// Streaming: each `send` goes out as it is made, so a long result never
    /// has to exist all at once. `try` it, and the handler stops when the
    /// client cancels or the deadline passes.
    pub fn sayHelloTimes(
        ctx: *zgrpc.CallContext,
        request: pb.HelloTimesRequest,
        sender: *zgrpc.typed.Sender(pb.HelloReply),
    ) anyerror!void {
        for (0..request.times) |i| {
            const message = try std.fmt.allocPrint(ctx.allocator, "hello {s} #{d}", .{ request.name, i + 1 });
            try sender.send(.{ .message = message });
        }
    }
};

const methods = stubs.Greeter.Methods(Greeter).all;

fn serve(server: *zgrpc.Server) void {
    server.serve() catch {};
}

/// The streaming client half: messages arrive one at a time, each valid until
/// the next call to `next`.
fn greetTimes(
    allocator: std.mem.Allocator,
    greeter: stubs.Greeter.Client,
    name: []const u8,
    times: u32,
) !void {
    var stream = try greeter.sayHelloTimes(allocator, .{ .name = name, .times = times }, .{});
    defer stream.deinit();

    while (try stream.next()) |reply| std.debug.print("{s: <7} -> {s}\n", .{ name, reply.message });
    if (!stream.finalStatus().isOk()) std.debug.print("{s: <7} -> {t}\n", .{ name, stream.finalStatus().code });
}

/// The client half: a typed call, a typed reply.
fn greet(
    allocator: std.mem.Allocator,
    greeter: stubs.Greeter.Client,
    name: []const u8,
) !void {
    var reply = try greeter.sayHello(allocator, .{ .name = name }, .{
        .deadline_ns = 2 * std.time.ns_per_s,
    });
    defer reply.deinit();

    if (reply.message) |message| {
        std.debug.print("{s: <7} -> {s}\n", .{ name, message.message });
    } else {
        std.debug.print("{s: <7} -> {t}: {s}\n", .{ name, reply.status.code, reply.status.message });
    }
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server = try zgrpc.Server.listen(
        allocator,
        io,
        try std.Io.net.IpAddress.parse("127.0.0.1", 0),
        &methods,
        .{},
    );
    defer server.deinit();

    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    var serving = try io.concurrent(serve, .{&server});
    defer _ = serving.cancel(io);

    const pool = try zgrpc.Pool.init(allocator, io, bound, authority, .{});
    defer pool.deinit();

    const greeter = stubs.Greeter.Client.init(pool);
    try greet(allocator, greeter, "zig");
    try greet(allocator, greeter, "world");
    // An empty name, so the handler's `ctx.fail` comes back as a gRPC status
    // rather than as a transport error.
    try greet(allocator, greeter, "");
    try greetTimes(allocator, greeter, "stream", 3);
}
