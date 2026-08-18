//! zgrpc with protobuf messages, end to end over a real socket.
//!
//! A standalone project rather than part of the main build, and that is the
//! point: zgrpc has no protobuf dependency, so anything demonstrating protobuf
//! has to bring its own. Keeping it out here is what makes that true rather
//! than merely claimed — see the repository README, "Protobuf".
//!
//!     zig build run        the round trip below
//!     zig build gen-proto  regenerate src/helloworld.pb.zig from proto/

const std = @import("std");
const zgrpc = @import("zgrpc");
const pb = @import("helloworld.pb.zig");

/// Written out by hand to match proto/helloworld.proto. Nothing generates or
/// checks it, which is the gap described in the README.
const say_hello = "/helloworld.Greeter/SayHello";

/// zgrpc hands over the serialized message and wants one back, so a handler is
/// decode, work, encode. Everything comes from `ctx.allocator`, an arena freed
/// when the call ends, so none of it needs cleanup.
fn sayHello(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    var reader = std.Io.Reader.fixed(request);
    const req = try pb.HelloRequest.decode(&reader, ctx.allocator);

    if (req.name.len == 0) return ctx.fail(.invalid_argument, "name is required");

    const reply: pb.HelloReply = .{
        .message = try std.fmt.allocPrint(ctx.allocator, "hello {s}", .{req.name}),
    };

    var out: std.Io.Writer.Allocating = .init(ctx.allocator);
    try reply.encode(&out.writer, ctx.allocator);
    return out.written();
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary(say_hello, sayHello),
};

fn serve(server: *zgrpc.Server) void {
    server.serve() catch {};
}

/// The client half: encode, call, decode.
fn greet(
    allocator: std.mem.Allocator,
    pool: *zgrpc.Pool,
    name: []const u8,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var request: std.Io.Writer.Allocating = .init(arena.allocator());
    try (pb.HelloRequest{ .name = name }).encode(&request.writer, arena.allocator());

    var response = try pool.call(say_hello, request.written(), .{
        .deadline_ns = 2 * std.time.ns_per_s,
    });
    defer response.deinit();

    if (!response.call_status.isOk()) {
        std.debug.print("{s: <7} -> {t}: {s}\n", .{
            name,
            response.call_status.code,
            response.call_status.message,
        });
        return;
    }

    var reader = std.Io.Reader.fixed(response.message);
    const reply = try pb.HelloReply.decode(&reader, arena.allocator());
    std.debug.print("{s: <7} -> {s}\n", .{ name, reply.message });
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

    try greet(allocator, pool, "zig");
    try greet(allocator, pool, "world");
    // An empty name, so the handler's `ctx.fail` comes back as a gRPC status
    // rather than as a transport error.
    try greet(allocator, pool, "");
}
