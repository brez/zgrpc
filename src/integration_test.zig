//! End-to-end tests: a real client and a real server talking h2c over a real
//! TCP socket on loopback.

const std = @import("std");
const zgrpc = @import("root.zig");

const testing = std.testing;

const echo_path = "/zgrpc.test.Echo/Call";
const fail_path = "/zgrpc.test.Echo/Fail";

fn echoHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    return ctx.allocator.dupe(u8, request);
}

fn failHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    return ctx.fail(.not_found, "no such widget");
}

const methods = [_]zgrpc.Method{
    .{ .path = echo_path, .handler = echoHandler },
    .{ .path = fail_path, .handler = failHandler },
};

const Harness = struct {
    threaded: *std.Io.Threaded,
    io: std.Io,
    server: zgrpc.Server,
    thread: std.Thread,
    authority: [64]u8,
    authority_len: usize,

    fn serveThread(server: *zgrpc.Server) void {
        server.acceptAndServe() catch |err| switch (err) {
            error.EndOfStream, error.ConnectionResetByPeer => {},
            else => std.debug.print("server error: {t}\n", .{err}),
        };
    }
};

/// Starts a server on an ephemeral port and returns a connected client.
/// The server handles exactly one connection, then the thread exits.
fn startPair(
    allocator: std.mem.Allocator,
    threaded: *std.Io.Threaded,
) !struct {
    io: std.Io,
    server: *zgrpc.Server,
    thread: std.Thread,
    client: *zgrpc.Client,
    authority: []u8,
} {
    const io = threaded.io();

    const server = try allocator.create(zgrpc.Server);
    errdefer allocator.destroy(server);
    server.* = try zgrpc.Server.listen(
        allocator,
        io,
        std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
        &methods,
        .{},
    );

    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    errdefer allocator.free(authority);

    const thread = try std.Thread.spawn(.{}, Harness.serveThread, .{server});

    const client = try zgrpc.Client.connect(allocator, io, bound, authority, .{});

    return .{
        .io = io,
        .server = server,
        .thread = thread,
        .client = client,
        .authority = authority,
    };
}

test "unary call round-trips a message over real h2c" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    var response = try pair.client.call(echo_path, "hello zgrpc", .{});
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
    try testing.expectEqualStrings("hello zgrpc", response.message);
}

test "several calls reuse one connection" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    // Each call must land on a new stream id over the same connection.
    for (0..5) |i| {
        var buf: [32]u8 = undefined;
        const payload = try std.fmt.bufPrint(&buf, "message {d}", .{i});

        var response = try pair.client.call(echo_path, payload, .{});
        defer response.deinit();

        try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
        try testing.expectEqualStrings(payload, response.message);
    }
}

test "handler failure arrives as a grpc status, not a transport error" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    var response = try pair.client.call(fail_path, "", .{});
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.not_found, response.call_status.code);
    try testing.expectEqualStrings("no such widget", response.call_status.message);
}

test "unknown method returns UNIMPLEMENTED" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    var response = try pair.client.call("/nope.Nope/Nope", "x", .{});
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.unimplemented, response.call_status.code);
}

test "deadline is propagated to the server as grpc-timeout" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    // The echo handler reports whether it saw a deadline.
    var response = try pair.client.call(echo_path, "deadline", .{
        .deadline_ns = 5 * std.time.ns_per_ms,
    });
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
    try testing.expectEqualStrings("deadline", response.message);
}

test "an empty request and response are legal" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    var response = try pair.client.call(echo_path, "", .{});
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
    try testing.expectEqualStrings("", response.message);
}

test "a message larger than one HTTP/2 frame is reassembled" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    // 128KB exceeds the 16KB max frame size and, more importantly, exceeds
    // the 64KB default flow-control window in both directions.
    const big = try allocator.alloc(u8, 128 * 1024);
    defer allocator.free(big);
    for (big, 0..) |*b, i| b.* = @intCast(i % 251);

    var response = try pair.client.call(echo_path, big, .{});
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
    try testing.expectEqualSlices(u8, big, response.message);
}
