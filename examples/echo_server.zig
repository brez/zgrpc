//! A standalone echo server, used both as an example and as the target for
//! the Go-interop test (`interop/` — verifies we speak real gRPC, not just a
//! dialect our own client happens to agree with).
//!
//! Binds an ephemeral port and prints it on stdout so a test harness can pick
//! it up: `listening <port>`.

const std = @import("std");
const zgrpc = @import("zgrpc");

fn echo(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    return ctx.allocator.dupe(u8, request);
}

fn fail(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    return ctx.fail(.permission_denied, "denied by policy");
}

/// Streams `request` (a decimal count) messages back, one per item.
fn count(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = ctx;
    const n = std.fmt.parseInt(usize, request, 10) catch 0;
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        try stream.send(try std.fmt.bufPrint(&buf, "item-{d}", .{i}));
    }
}

/// Reports the milliseconds left on the call, so an interop client can check
/// that the `grpc-timeout` it chose to send arrived as a usable deadline.
fn deadlineReport(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    const left = ctx.remaining() orelse return ctx.allocator.dupe(u8, "none");
    return std.fmt.allocPrint(ctx.allocator, "{d}", .{@divTrunc(left.nanoseconds, std.time.ns_per_ms)});
}

/// Echoes after a fixed pause, so a caller can tell concurrent handling from
/// sequential handling by the clock: N of these at once should take about as
/// long as one, not N times as long.
fn slow(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    try std.Io.sleep(ctx.io, .fromMilliseconds(300), .awake);
    return ctx.allocator.dupe(u8, request);
}

/// Emits two messages and then fails, so the status has to travel in trailers
/// after a body rather than as a Trailers-Only response.
fn countThenFail(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = request;
    try stream.send("first");
    try stream.send("second");
    return ctx.fail(.data_loss, "ran out of data");
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary("/zgrpc.test.Echo/Call", echo),
    zgrpc.Method.unary("/zgrpc.test.Echo/Fail", fail),
    zgrpc.Method.unary("/zgrpc.test.Echo/Slow", slow),
    zgrpc.Method.unary("/zgrpc.test.Echo/DeadlineReport", deadlineReport),
    zgrpc.Method.serverStream("/zgrpc.test.Echo/Count", count),
    zgrpc.Method.serverStream("/zgrpc.test.Echo/CountThenFail", countThenFail),
};

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

    // The harness waits on this line before starting the client.
    var out_buf: [64]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(io, &out_buf);
    try stdout.interface.print("listening {d}\n", .{server.boundAddress().getPort()});
    try stdout.interface.flush();

    // One task per connection; see the concurrency note in server.zig.
    try server.serve();
}
