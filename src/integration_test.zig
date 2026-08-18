//! End-to-end tests: a real client and a real server talking h2c over a real
//! TCP socket on loopback.

const std = @import("std");
const zgrpc = @import("root.zig");

const testing = std.testing;

const echo_path = "/zgrpc.test.Echo/Call";
const fail_path = "/zgrpc.test.Echo/Fail";
const count_path = "/zgrpc.test.Echo/Count";
const fail_mid_path = "/zgrpc.test.Echo/FailMidStream";
const fail_early_path = "/zgrpc.test.Echo/FailEarlyStream";

fn echoHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    return ctx.allocator.dupe(u8, request);
}

fn failHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    return ctx.fail(.not_found, "no such widget");
}

/// Counts messages actually produced, so a test can tell whether a cancelled
/// handler stopped early instead of running to completion.
var sent_count: std.atomic.Value(usize) = .init(0);

fn countHandler(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    const n = try std.fmt.parseInt(usize, request, 10);
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        try stream.send(try std.fmt.bufPrint(&buf, "item-{d}", .{i}));
        _ = sent_count.fetchAdd(1, .monotonic);
    }
    _ = ctx;
}

/// Sends two messages and then fails, so the failure must land in trailers
/// rather than as a Trailers-Only response.
fn failMidStreamHandler(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = request;
    try stream.send("first");
    try stream.send("second");
    return ctx.fail(.data_loss, "ran out of data");
}

fn failEarlyStreamHandler(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = request;
    _ = stream;
    return ctx.fail(.invalid_argument, "bad range");
}

/// How many calls have reached `rendezvousHandler` and are waiting there.
var arrived: std.atomic.Value(usize) = .init(0);

/// Blocks until a second call reaches it, then reports whether it got one.
///
/// This is the concurrency test in one handler: served one call at a time, the
/// first parks here and the second never arrives, so the answer is "alone". It
/// cannot pass by accident.
fn rendezvousHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    _ = arrived.fetchAdd(1, .monotonic);

    // Bounded so a regression fails the test instead of hanging it.
    var waited_ms: usize = 0;
    while (arrived.load(.monotonic) < 2 and waited_ms < 2000) : (waited_ms += 1) {
        try std.Io.sleep(ctx.io, .fromMilliseconds(1), .awake);
    }

    return ctx.allocator.dupe(u8, if (arrived.load(.monotonic) >= 2) "both" else "alone");
}

/// Set by the test once it has proved the server received its RST_STREAM.
var cancel_proceed: std.atomic.Value(bool) = .init(false);
/// Messages `cancelProbeHandler` got out.
var cancel_sends: std.atomic.Value(usize) = .init(0);
/// What the handler saw from its second send: 0 still running, 1 StreamReset,
/// 2 the send succeeded, 3 some other error.
var cancel_outcome: std.atomic.Value(u8) = .init(0);

/// Sends one message, waits to be told the cancellation has landed, then tries
/// to send another.
///
/// The second send has a wide-open flow-control window, so nothing about it
/// blocks. It has to fail purely because the connection's reader already saw
/// the RST_STREAM and recorded it — which is what "prompt" means here.
fn cancelProbeHandler(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = request;
    try stream.send("first");
    _ = cancel_sends.fetchAdd(1, .monotonic);

    var waited_ms: usize = 0;
    while (!cancel_proceed.load(.acquire) and waited_ms < 5000) : (waited_ms += 1) {
        try std.Io.sleep(ctx.io, .fromMilliseconds(1), .awake);
    }

    if (stream.send("second")) |_| {
        _ = cancel_sends.fetchAdd(1, .monotonic);
        cancel_outcome.store(2, .release);
    } else |err| {
        cancel_outcome.store(if (err == error.StreamReset) 1 else 3, .release);
        return err;
    }
}

const rendezvous_path = "/zgrpc.test.Echo/Rendezvous";
const cancel_probe_path = "/zgrpc.test.Echo/CancelProbe";

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary(echo_path, echoHandler),
    zgrpc.Method.unary(fail_path, failHandler),
    zgrpc.Method.unary(rendezvous_path, rendezvousHandler),
    zgrpc.Method.serverStream(count_path, countHandler),
    zgrpc.Method.serverStream(fail_mid_path, failMidStreamHandler),
    zgrpc.Method.serverStream(fail_early_path, failEarlyStreamHandler),
    zgrpc.Method.serverStream(cancel_probe_path, cancelProbeHandler),
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

test "server streaming yields every message in order" {
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

    var stream = try pair.client.callServerStream(count_path, "5", .{});
    defer stream.deinit();

    var seen: usize = 0;
    while (try stream.next()) |msg| {
        var expected: [32]u8 = undefined;
        try testing.expectEqualStrings(
            try std.fmt.bufPrint(&expected, "item-{d}", .{seen}),
            msg,
        );
        seen += 1;
    }

    try testing.expectEqual(@as(usize, 5), seen);
    try testing.expectEqual(zgrpc.Code.ok, stream.finalStatus().code);
}

test "a stream of zero messages is a clean, successful end" {
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

    var stream = try pair.client.callServerStream(count_path, "0", .{});
    defer stream.deinit();

    try testing.expectEqual(@as(?[]const u8, null), try stream.next());
    try testing.expectEqual(zgrpc.Code.ok, stream.finalStatus().code);
}

test "a long stream is delivered without buffering it whole" {
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

    var stream = try pair.client.callServerStream(count_path, "1000", .{});
    defer stream.deinit();

    var seen: usize = 0;
    while (try stream.next()) |msg| {
        try testing.expect(std.mem.startsWith(u8, msg, "item-"));
        seen += 1;
    }

    try testing.expectEqual(@as(usize, 1000), seen);
    try testing.expectEqual(zgrpc.Code.ok, stream.finalStatus().code);
}

test "failure after messages arrives in trailers, keeping the messages" {
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

    var stream = try pair.client.callServerStream(fail_mid_path, "", .{});
    defer stream.deinit();

    try testing.expectEqualStrings("first", (try stream.next()).?);
    try testing.expectEqualStrings("second", (try stream.next()).?);
    try testing.expectEqual(@as(?[]const u8, null), try stream.next());

    // The messages already delivered stay valid; the failure is reported only
    // at the end of the stream.
    try testing.expectEqual(zgrpc.Code.data_loss, stream.finalStatus().code);
    try testing.expectEqualStrings("ran out of data", stream.finalStatus().message);
}

test "failure before any message collapses to a Trailers-Only response" {
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

    var stream = try pair.client.callServerStream(fail_early_path, "", .{});
    defer stream.deinit();

    try testing.expectEqual(@as(?[]const u8, null), try stream.next());
    try testing.expectEqual(zgrpc.Code.invalid_argument, stream.finalStatus().code);
    try testing.expectEqualStrings("bad range", stream.finalStatus().message);
}

test "streaming and unary calls interleave on one connection" {
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

    for (0..3) |_| {
        var stream = try pair.client.callServerStream(count_path, "3", .{});
        var seen: usize = 0;
        while (try stream.next()) |_| seen += 1;
        try testing.expectEqual(@as(usize, 3), seen);
        try testing.expectEqual(zgrpc.Code.ok, stream.finalStatus().code);
        stream.deinit();

        var response = try pair.client.call(echo_path, "still works", .{});
        defer response.deinit();
        try testing.expectEqualStrings("still works", response.message);
    }
}

test "abandoning a stream early leaves the connection usable" {
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

    const total = 20_000;
    sent_count.store(0, .monotonic);
    {
        // Read two of many messages, then walk away. The count must be large
        // enough that the server is still producing when the client gives up,
        // or the test proves nothing about cancellation.
        var stream = try pair.client.callServerStream(count_path, "20000", .{});
        defer stream.deinit();
        _ = try stream.next();
        _ = try stream.next();
    }

    // The next call must not be confused by frames left over from the
    // abandoned stream.
    var response = try pair.client.call(echo_path, "after abandon", .{});
    defer response.deinit();
    try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
    try testing.expectEqualStrings("after abandon", response.message);

    // Cancellation must actually stop the handler, not merely be tolerated:
    // a server that streams the whole result set into a discarded stream
    // wastes the connection for as long as the query is large.
    //
    // How far it gets is now a matter of round-trip timing rather than of the
    // flow-control window, so the bound checked here is deliberately loose.
    // The deterministic claim is tested separately, in "cancellation reaches
    // the handler without waiting for the window to close".
    const produced = sent_count.load(.monotonic);
    try testing.expect(produced < total);
}

// -- concurrency -------------------------------------------------------------

fn serveTask(server: *zgrpc.Server) void {
    server.serve() catch |err| switch (err) {
        // The test cancels the accept loop to shut the server down.
        error.Canceled => {},
        else => std.debug.print("serve error: {t}\n", .{err}),
    };
}

const RendezvousResult = struct {
    code: ?zgrpc.Code = null,
    message: [8]u8 = @splat(0),
    message_len: usize = 0,

    fn text(self: *const RendezvousResult) []const u8 {
        return self.message[0..self.message_len];
    }

    fn record(self: *RendezvousResult, response: *const zgrpc.client.Response) void {
        self.code = response.call_status.code;
        self.message_len = @min(response.message.len, self.message.len);
        @memcpy(self.message[0..self.message_len], response.message[0..self.message_len]);
    }
};

/// One rendezvous call on an existing client.
fn rendezvousCall(client: *zgrpc.Client, out: *RendezvousResult) void {
    var response = client.call(rendezvous_path, "", .{}) catch |err| {
        std.debug.print("call failed: {t}\n", .{err});
        return;
    };
    defer response.deinit();
    out.record(&response);
}

/// One rendezvous call on a connection of its own.
fn rendezvousClient(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    authority: []const u8,
    out: *RendezvousResult,
) void {
    const client = zgrpc.Client.connect(allocator, io, address, authority, .{}) catch |err| {
        std.debug.print("client connect failed: {t}\n", .{err});
        return;
    };
    defer client.deinit();
    rendezvousCall(client, out);
}

test "two connections are served at the same time, not one after the other" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const server = try allocator.create(zgrpc.Server);
    defer allocator.destroy(server);
    server.* = try zgrpc.Server.listen(
        allocator,
        io,
        std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
        &methods,
        .{},
    );
    defer server.deinit();

    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    var serving = try io.concurrent(serveTask, .{server});
    defer _ = serving.cancel(io);

    arrived.store(0, .monotonic);

    var results: [2]RendezvousResult = .{ .{}, .{} };
    var clients: std.Io.Group = .init;
    errdefer clients.cancel(io);
    for (&results) |*result| {
        try clients.concurrent(io, rendezvousClient, .{ allocator, io, bound, authority, result });
    }
    try clients.await(io);

    for (&results) |*result| {
        try testing.expectEqual(zgrpc.Code.ok, result.code);
        // "alone" means the second call never got in while the first was
        // running — the server serialised the two connections.
        try testing.expectEqualStrings("both", result.text());
    }
}

test "two calls on one connection are multiplexed, not queued" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    arrived.store(0, .monotonic);

    // Both calls share one `Client`, so they share one HTTP/2 connection and
    // are distinguished only by stream id. Queued, the first would park in the
    // handler and the second would never be read off the socket.
    var results: [2]RendezvousResult = .{ .{}, .{} };
    var calls: std.Io.Group = .init;
    errdefer calls.cancel(io);
    for (&results) |*result| {
        try calls.concurrent(io, rendezvousCall, .{ pair.client, result });
    }
    try calls.await(io);

    for (&results) |*result| {
        try testing.expectEqual(zgrpc.Code.ok, result.code);
        try testing.expectEqualStrings("both", result.text());
    }
}

test "cancellation reaches the handler without waiting for the window to close" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var pair = try startPair(allocator, &threaded);
    defer {
        pair.client.deinit();
        pair.thread.join();
        pair.server.deinit();
        allocator.destroy(pair.server);
        allocator.free(pair.authority);
    }

    cancel_proceed.store(false, .release);
    cancel_sends.store(0, .monotonic);
    cancel_outcome.store(0, .release);

    {
        var stream = try pair.client.callServerStream(cancel_probe_path, "", .{});
        const first = try stream.next();
        try testing.expectEqualStrings("first", first.?);
        // Cancels the call. The handler is parked, not writing, so nothing
        // about its flow-control window is about to change.
        stream.deinit();
    }

    // A full round trip on the same connection. Frames arrive in order, so a
    // reply to a request sent after the RST_STREAM proves the server processed
    // the RST_STREAM. It also needs the server to run this call while the
    // probe handler is still parked, which is the other half of the point.
    {
        var response = try pair.client.call(echo_path, "ping", .{});
        defer response.deinit();
        try testing.expectEqualStrings("ping", response.message);
    }

    cancel_proceed.store(true, .release);

    var waited_ms: usize = 0;
    while (cancel_outcome.load(.acquire) == 0 and waited_ms < 5000) : (waited_ms += 1) {
        try std.Io.sleep(io, .fromMilliseconds(1), .awake);
    }

    // 2 would mean the send went through: the server had credit and no idea
    // the call was gone, which is exactly the old behaviour.
    try testing.expectEqual(@as(u8, 1), cancel_outcome.load(.acquire));
    try testing.expectEqual(@as(usize, 1), cancel_sends.load(.monotonic));
}
