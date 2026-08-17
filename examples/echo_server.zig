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

const methods = [_]zgrpc.Method{
    .{ .path = "/zgrpc.test.Echo/Call", .handler = echo },
    .{ .path = "/zgrpc.test.Echo/Fail", .handler = fail },
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

    // Serve connections sequentially; see the concurrency note in server.zig.
    while (true) {
        server.acceptAndServe() catch |err| switch (err) {
            error.EndOfStream, error.ConnectionResetByPeer => continue,
            else => return err,
        };
    }
}
