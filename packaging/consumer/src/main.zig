//! Smoke test for zgrpc consumed as a published package.
//!
//! Deliberately uses the surface a real consumer would: a server with both
//! kinds of method, a pool rather than a bare client, a deadline, a non-OK
//! status, and a stream drained to completion — over a real socket. Anything
//! the package fails to export, or exports in a shape that does not compile
//! from outside, fails here.
//!
//! Run via `packaging/verify.sh`, which points it at a tarball of the working
//! tree rather than at this checkout.

const std = @import("std");
const zgrpc = @import("zgrpc");

fn greet(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    if (request.len == 0) return ctx.fail(.invalid_argument, "empty request");
    return std.fmt.allocPrint(ctx.allocator, "hello {s}", .{request});
}

fn count(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = ctx;
    const n = try std.fmt.parseInt(usize, request, 10);
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        try stream.send(try std.fmt.bufPrint(&buf, "row-{d}", .{i}));
    }
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary("/demo.Greeter/SayHello", greet),
    zgrpc.Method.serverStream("/demo.Greeter/Count", count),
};

fn serve(server: *zgrpc.Server) void {
    server.serve() catch {};
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

    var failures: usize = 0;

    {
        var response = try pool.call("/demo.Greeter/SayHello", "world", .{
            .deadline_ns = 2 * std.time.ns_per_s,
        });
        defer response.deinit();
        if (!std.mem.eql(u8, response.message, "hello world")) failures += 1;
        std.debug.print("unary:     {s}\n", .{response.message});
    }

    {
        var response = try pool.call("/demo.Greeter/SayHello", "", .{});
        defer response.deinit();
        if (response.call_status.code != .invalid_argument) failures += 1;
        std.debug.print("status:    {t} {s}\n", .{ response.call_status.code, response.call_status.message });
    }

    {
        var stream = try pool.callServerStream("/demo.Greeter/Count", "3", .{});
        defer stream.deinit();
        var seen: usize = 0;
        while (try stream.next()) |message| : (seen += 1) {
            std.debug.print("stream[{d}]: {s}\n", .{ seen, message });
        }
        if (seen != 3 or !stream.finalStatus().isOk()) failures += 1;
    }

    if (failures > 0) {
        std.debug.print("FAILED: {d} check(s)\n", .{failures});
        std.process.exit(1);
    }
    std.debug.print("consumer smoke test passed\n", .{});
}
