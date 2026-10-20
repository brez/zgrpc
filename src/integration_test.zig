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

/// Answers with the metadata the client sent, so a test can see exactly what
/// crossed the wire. Absent entries are reported as such, not as empty.
fn metadataHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    return std.fmt.allocPrint(ctx.allocator, "id={s};auth={s}", .{
        ctx.metadata("x-request-id") orelse "<none>",
        ctx.metadata("authorization") orelse "<none>",
    });
}

const metadata_path = "/zgrpc.test.Echo/Metadata";
const rendezvous_path = "/zgrpc.test.Echo/Rendezvous";
const cancel_probe_path = "/zgrpc.test.Echo/CancelProbe";

/// Reports how many milliseconds the call has left, so a test can see that
/// `grpc-timeout` survived the round trip as something the handler can act on.
fn deadlineReportHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    const left = ctx.remaining() orelse return ctx.allocator.dupe(u8, "none");
    return std.fmt.allocPrint(ctx.allocator, "{d}", .{@divTrunc(left.nanoseconds, std.time.ns_per_ms)});
}

/// Overruns any deadline a test sets, without checking for one.
fn slowHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    try std.Io.sleep(ctx.io, .fromMilliseconds(1000), .awake);
    return ctx.allocator.dupe(u8, request);
}

/// What ended `stalledStreamHandler`: 0 still running, 1 its deadline, 2
/// cancellation, 3 something else, 4 nothing — it ran to completion.
var stalled_outcome: std.atomic.Value(u8) = .init(0);

/// Streams more than any flow-control window will hold, so it blocks for
/// credit as soon as the client stops reading.
fn stalledStreamHandler(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    _ = request;
    const chunk = try ctx.allocator.alloc(u8, 64 * 1024);
    @memset(chunk, 'x');

    for (0..64) |_| {
        stream.send(chunk) catch |err| {
            stalled_outcome.store(switch (err) {
                error.DeadlineExceeded => 1,
                error.StreamReset => 2,
                else => 3,
            }, .release);
            return err;
        };
    }
    stalled_outcome.store(4, .release);
}

const deadline_report_path = "/zgrpc.test.Echo/DeadlineReport";
const slow_path = "/zgrpc.test.Echo/Slow";
const stalled_path = "/zgrpc.test.Echo/Stalled";

/// Fails with UNAVAILABLE for its first `flaky_failures` calls, then succeeds.
/// Tests set both globals before use; they run one at a time.
const flaky_path = "/zgrpc.test.Echo/Flaky";
var flaky_calls: std.atomic.Value(u32) = .init(0);
var flaky_failures: std.atomic.Value(u32) = .init(0);

fn flakyHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    const n = flaky_calls.fetchAdd(1, .monotonic);
    if (n < flaky_failures.load(.monotonic)) return ctx.fail(.unavailable, "try again");
    return ctx.allocator.dupe(u8, "ok");
}

const methods = [_]zgrpc.Method{
    zgrpc.Method.unary(flaky_path, flakyHandler),
    zgrpc.Method.unary(echo_path, echoHandler),
    zgrpc.Method.unary(metadata_path, metadataHandler),
    zgrpc.Method.unary(deadline_report_path, deadlineReportHandler),
    zgrpc.Method.unary(slow_path, slowHandler),
    zgrpc.Method.serverStream(stalled_path, stalledStreamHandler),
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
) !PairType {
    return startPairWith(allocator, threaded, .{});
}

const PairType = struct {
    io: std.Io,
    server: *zgrpc.Server,
    thread: std.Thread,
    client: *zgrpc.Client,
    authority: []u8,
};

fn startPairWith(
    allocator: std.mem.Allocator,
    threaded: *std.Io.Threaded,
    options: zgrpc.server.Options,
) !PairType {
    const io = threaded.io();

    const server = try allocator.create(zgrpc.Server);
    errdefer allocator.destroy(server);
    server.* = try zgrpc.Server.listen(
        allocator,
        io,
        std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
        &methods,
        options,
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

const Counter = struct { calls: std.atomic.Value(u32) = .init(0) };

fn userDataHandler(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    _ = request;
    const counter = ctx.userData(Counter);
    const n = counter.calls.fetchAdd(1, .monotonic) + 1;
    return std.fmt.allocPrint(ctx.allocator, "{d}", .{n});
}

test "handlers reach the state the server was built with" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();

    var counter: Counter = .{};
    const user_methods = [_]zgrpc.Method{zgrpc.Method.unary("/zgrpc.test.State/Bump", userDataHandler)};

    const io = threaded.io();
    var server = try zgrpc.Server.listen(
        allocator,
        io,
        std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
        &user_methods,
        .{ .user_data = &counter },
    );
    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    const thread = try std.Thread.spawn(.{}, Harness.serveThread, .{&server});
    const client = try zgrpc.Client.connect(allocator, io, bound, authority, .{});
    defer {
        client.deinit();
        thread.join();
        server.deinit();
    }

    for (1..4) |i| {
        var response = try client.call("/zgrpc.test.State/Bump", "", .{});
        defer response.deinit();
        var expected: [8]u8 = undefined;
        try testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "{d}", .{i}), response.message);
    }
    try testing.expectEqual(@as(u32, 3), counter.calls.load(.monotonic));
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

    var response = try pair.client.call(deadline_report_path, "", .{
        .deadline_ns = 5 * std.time.ns_per_s,
    });
    defer response.deinit();

    try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);

    // The handler reports the time it has left. That it is positive and no
    // more than what was asked for is the whole claim: `grpc-timeout` was
    // encoded, sent, decoded, and turned into a clock the handler can read.
    const left_ms = try std.fmt.parseInt(i64, response.message, 10);
    try testing.expect(left_ms > 0);
    try testing.expect(left_ms <= 5000);
}

test "a client stops waiting at its deadline instead of at the handler's convenience" {
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

    const started = std.Io.Timestamp.now(io, .awake);
    const result = pair.client.call(slow_path, "", .{ .deadline_ns = 200 * std.time.ns_per_ms });
    const elapsed_ms = @divTrunc(
        std.Io.Timestamp.now(io, .awake).nanoseconds - started.nanoseconds,
        std.time.ns_per_ms,
    );

    try testing.expectError(error.DeadlineExceeded, result);
    // The handler sleeps a second. Returning in a fraction of that is the
    // point: the deadline is the client's own, not something it discovers when
    // the server finally answers.
    try testing.expect(elapsed_ms < 800);
}

test "the server stops a handler at the deadline when the client has stopped reading" {
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

    stalled_outcome.store(0, .release);

    var stream = try pair.client.callServerStream(stalled_path, "", .{
        .deadline_ns = 500 * std.time.ns_per_ms,
    });
    defer stream.deinit();

    // One message, so the call is definitely running, and then stop reading.
    // The server fills the flow-control window and blocks for credit that is
    // never coming — with nothing cancelled, so only its own deadline can end
    // it. This is the case the server-side deadline exists for: a client that
    // has gone quiet rather than gone away.
    const first = try stream.next();
    try testing.expect(first != null);

    try std.Io.sleep(io, .fromMilliseconds(2000), .awake);

    // 2 would mean it was cancellation that stopped the handler rather than
    // the deadline, which would make this test prove nothing.
    try testing.expectEqual(@as(u8, 1), stalled_outcome.load(.acquire));
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

// -- connection pool ---------------------------------------------------------

/// A pool call on an existing pool, for running several at once.
fn poolRendezvousCall(p: *zgrpc.Pool, out: *RendezvousResult) void {
    var response = p.call(rendezvous_path, "", .{}) catch |err| {
        std.debug.print("pool call failed: {t}\n", .{err});
        return;
    };
    defer response.deinit();
    out.record(&response);
}

/// A server on its own task, plus a pool pointed at it.
const PoolFixture = struct {
    server: *zgrpc.Server,
    serving: std.Io.Future(void),
    authority: []u8,
    bound: std.Io.net.IpAddress,
    allocator: std.mem.Allocator,
    io: std.Io,

    fn start(allocator: std.mem.Allocator, io: std.Io) !PoolFixture {
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

        return .{
            .server = server,
            .serving = try io.concurrent(serveTask, .{server}),
            .authority = authority,
            .bound = bound,
            .allocator = allocator,
            .io = io,
        };
    }

    /// Shuts the server down but keeps the address and authority, which a
    /// pool pointed at this fixture has borrowed and is still using.
    fn stopServer(self: *PoolFixture) void {
        _ = self.serving.cancel(self.io);
        self.server.deinit();
        self.allocator.destroy(self.server);
    }

    fn deinit(self: *PoolFixture) void {
        self.allocator.free(self.authority);
    }
};

test "a pool serves sequential calls on one connection however large it may grow" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();

    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{ .size = 4 });
    defer pool.deinit();

    for (0..5) |i| {
        var buf: [32]u8 = undefined;
        const payload = try std.fmt.bufPrint(&buf, "message {d}", .{i});

        var response = try pool.call(echo_path, payload, .{});
        defer response.deinit();
        try testing.expectEqualStrings(payload, response.message);
    }

    // Capacity is a ceiling, not a target: nothing was ever busy, so nothing
    // beyond the first connection was ever worth opening.
    try testing.expectEqual(@as(usize, 1), pool.connectionCount());
}

test "a pool opens a second connection only when the first is busy" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();

    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{ .size = 4 });
    defer pool.deinit();

    arrived.store(0, .monotonic);

    // Both handlers park until the other arrives, so the first connection is
    // provably still busy when the second call goes looking for one.
    var results: [2]RendezvousResult = .{ .{}, .{} };
    var calls: std.Io.Group = .init;
    errdefer calls.cancel(io);
    for (&results) |*result| {
        try calls.concurrent(io, poolRendezvousCall, .{ pool, result });
    }
    try calls.await(io);

    for (&results) |*result| {
        try testing.expectEqual(zgrpc.Code.ok, result.code);
        try testing.expectEqualStrings("both", result.text());
    }
    try testing.expectEqual(@as(usize, 2), pool.connectionCount());
}

test "a pool replaces a connection the server has closed" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    // The authority outlives the server here: the pool borrows it, and this
    // test deliberately takes the server away underneath it.
    defer fixture.deinit();

    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{});
    defer pool.deinit();

    {
        var response = try pool.call(echo_path, "before", .{});
        defer response.deinit();
        try testing.expectEqualStrings("before", response.message);
    }

    // Drop the server and stand a new one up on the same port, so the pool's
    // connection is dead but the peer is reachable again. A bare `Client`
    // would be finished at this point; the pool's job is to notice and
    // reconnect.
    const port = fixture.bound.getPort();
    fixture.stopServer();

    var replacement = try allocator.create(zgrpc.Server);
    defer allocator.destroy(replacement);
    replacement.* = try zgrpc.Server.listen(
        allocator,
        io,
        std.Io.net.IpAddress.parse("127.0.0.1", port) catch unreachable,
        &methods,
        .{},
    );
    defer replacement.deinit();
    var serving = try io.concurrent(serveTask, .{replacement});
    defer _ = serving.cancel(io);

    // The pool notices the dead connection when it next looks for one. The
    // first attempt may still land on the connection as it is dying, which is
    // a transport error and not something this library retries — so the check
    // is that the pool recovers, not that no call is ever lost.
    var recovered = false;
    for (0..10) |_| {
        var response = pool.call(echo_path, "after", .{}) catch continue;
        defer response.deinit();
        try testing.expectEqualStrings("after", response.message);
        recovered = true;
        break;
    }
    try testing.expect(recovered);
    try testing.expectEqual(@as(usize, 1), pool.connectionCount());
}

test "a pooled server-streaming call holds its connection until deinit" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();

    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{ .size = 4 });
    defer pool.deinit();

    {
        var stream = try pool.callServerStream(count_path, "3", .{});
        defer stream.deinit();

        // Unary calls made while the stream is open must not land on its
        // connection as if it were idle — the stream still holds it.
        var response = try pool.call(echo_path, "alongside", .{});
        defer response.deinit();
        try testing.expectEqualStrings("alongside", response.message);
        try testing.expectEqual(@as(usize, 2), pool.connectionCount());

        var seen: usize = 0;
        while (try stream.next()) |message| : (seen += 1) {
            var buf: [32]u8 = undefined;
            try testing.expectEqualStrings(
                try std.fmt.bufPrint(&buf, "item-{d}", .{seen}),
                message,
            );
        }
        try testing.expectEqual(@as(usize, 3), seen);
        try testing.expectEqual(zgrpc.Code.ok, stream.finalStatus().code);
    }

    // Released, so the next call reuses a connection rather than growing.
    var after = try pool.call(echo_path, "after", .{});
    defer after.deinit();
    try testing.expectEqualStrings("after", after.message);
    try testing.expectEqual(@as(usize, 2), pool.connectionCount());
}

// -- keepalive ---------------------------------------------------------------

const fast_keepalive: zgrpc.Keepalive = .{
    .interval_ns = 50 * std.time.ns_per_ms,
    .timeout_ns = 50 * std.time.ns_per_ms,
};

/// Connection slots the server has handed out. Racy by nature, so the tests
/// poll it rather than read it once.
fn slotsInUse(server: *zgrpc.Server, total: usize) usize {
    const io = server.io;
    server.slots.mutex.lockUncancelable(io);
    defer server.slots.mutex.unlock(io);
    return total - server.slots.permits;
}

/// Polls `slotsInUse` until it equals `want`, for at most `limit_ms`.
fn waitForSlots(server: *zgrpc.Server, total: usize, want: usize, limit_ms: usize) bool {
    var waited: usize = 0;
    while (waited < limit_ms) : (waited += 5) {
        if (slotsInUse(server, total) == want) return true;
        std.Io.sleep(server.io, .fromMilliseconds(5), .awake) catch return false;
    }
    return slotsInUse(server, total) == want;
}

test "a peer that goes silent is dropped, freeing its connection slot" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const slots = 4;
    const server = try allocator.create(zgrpc.Server);
    defer allocator.destroy(server);
    server.* = try zgrpc.Server.listen(
        allocator,
        io,
        std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
        &methods,
        .{ .max_concurrent_connections = slots, .keepalive = fast_keepalive },
    );
    defer server.deinit();

    var serving = try io.concurrent(serveTask, .{server});
    defer _ = serving.cancel(io);

    // A raw peer: does the handshake, then never reads and never answers. To
    // the server it is indistinguishable from a machine that fell off the
    // network.
    var addr = server.boundAddress();
    const sock = try addr.connect(io, .{ .mode = .stream });
    defer sock.close(io);
    var wbuf: [256]u8 = undefined;
    var writer = sock.writer(io, &wbuf);
    try writer.interface.writeAll(zgrpc.h2.client_preface);
    try writer.interface.writeAll(&.{ 0, 0, 0, 4, 0, 0, 0, 0, 0 }); // empty SETTINGS
    try writer.interface.flush();

    // `serve` holds one slot for the accept it is waiting on, so an idle
    // server reads 1 and one connected peer reads 2.
    try testing.expect(waitForSlots(server, slots, 2, 2000));
    // Without keepalive this never happens: the slot is held for good.
    try testing.expect(waitForSlots(server, slots, 1, 3000));
}

test "a peer that answers pings keeps its connection while idle" {
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
        .{ .keepalive = fast_keepalive },
    );
    defer server.deinit();
    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    var serving = try io.concurrent(serveTask, .{server});
    defer _ = serving.cancel(io);

    const client = try zgrpc.Client.connect(allocator, io, bound, authority, .{});
    defer client.deinit();

    // Idle for many keepalive periods: enough pings that a server which
    // mistook "no calls" for "no peer" would have hung up several times.
    try std.Io.sleep(io, .fromMilliseconds(600), .awake);

    var response = try client.call(echo_path, "still here", .{});
    defer response.deinit();
    try testing.expectEqualStrings("still here", response.message);
}

test "a client gives up on a server that stops answering" {
    const allocator = testing.allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Listening but never accepting or reading: the kernel completes the
    // handshake, so the client connects and then hears nothing, ever.
    var addr = std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable;
    var dead = try addr.listen(io, .{ .reuse_address = true });
    defer dead.deinit(io);
    const bound = dead.socket.address;
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    const client = try zgrpc.Client.connect(allocator, io, bound, authority, .{ .keepalive = fast_keepalive });
    defer client.deinit();

    // The deadline is only a backstop so a regression fails instead of
    // hanging; what is asserted is that keepalive got there first.
    const result = client.call(echo_path, "anyone?", .{ .deadline_ns = 3 * std.time.ns_per_s });
    try testing.expectError(error.KeepaliveTimeout, result);
}

// -- graceful shutdown -------------------------------------------------------

fn slowCallTask(client: *zgrpc.Client, finished_ok: *std.atomic.Value(bool)) void {
    var response = client.call(slow_path, "in flight", .{}) catch return;
    defer response.deinit();
    finished_ok.store(
        response.call_status.code == .ok and std.mem.eql(u8, response.message, "in flight"),
        .release,
    );
}

fn shutdownTask(server: *zgrpc.Server, grace_ns: u64, clean: *std.atomic.Value(u8)) void {
    clean.store(if (server.shutdown(grace_ns)) 1 else 2, .release);
}

const ShutdownFixture = struct {
    threaded: std.Io.Threaded,
    server: *zgrpc.Server,
    serving: std.Io.Future(void),
    client: *zgrpc.Client,
    authority: []u8,

    fn start(self: *ShutdownFixture, allocator: std.mem.Allocator) !void {
        self.threaded = std.Io.Threaded.init(allocator, .{});
        const io = self.threaded.io();
        self.server = try allocator.create(zgrpc.Server);
        self.server.* = try zgrpc.Server.listen(
            allocator,
            io,
            std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
            &methods,
            .{},
        );
        const bound = self.server.boundAddress();
        self.authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
        self.serving = try io.concurrent(serveTask, .{self.server});
        self.client = try zgrpc.Client.connect(allocator, io, bound, self.authority, .{});
    }

    fn deinit(self: *ShutdownFixture, allocator: std.mem.Allocator) void {
        self.client.deinit();
        self.server.deinit();
        allocator.destroy(self.server);
        allocator.free(self.authority);
        self.threaded.deinit();
    }
};

fn waitUnusable(client: *zgrpc.Client, io: std.Io, limit_ms: usize) bool {
    var waited: usize = 0;
    while (waited < limit_ms) : (waited += 5) {
        if (!client.isUsable()) return true;
        std.Io.sleep(io, .fromMilliseconds(5), .awake) catch return false;
    }
    return !client.isUsable();
}

test "shutdown lets a call in flight finish and refuses new ones" {
    const allocator = testing.allocator;
    var fx: ShutdownFixture = undefined;
    try fx.start(allocator);
    defer fx.deinit(allocator);
    const io = fx.threaded.io();

    var finished_ok: std.atomic.Value(bool) = .init(false);
    var calls: std.Io.Group = .init;
    try calls.concurrent(io, slowCallTask, .{ fx.client, &finished_ok });

    // Let the call reach the handler, which then sleeps for a second.
    try std.Io.sleep(io, .fromMilliseconds(150), .awake);

    _ = fx.serving.cancel(io);
    var clean: std.atomic.Value(u8) = .init(0);
    try calls.concurrent(io, shutdownTask, .{ fx.server, 5 * std.time.ns_per_s, &clean });

    // The client hears GOAWAY while its call is still running, and from then
    // on will not start another on this connection.
    try testing.expect(waitUnusable(fx.client, io, 500));
    try testing.expectEqual(@as(u8, 0), clean.load(.acquire)); // still draining
    // Refused before anything was sent, which is exactly what `Unprocessed`
    // says: a pool resends such a call on another connection without asking
    // whether the method is idempotent.
    try testing.expectError(error.Unprocessed, fx.client.call(echo_path, "late", .{}));

    try calls.await(io);
    // The point of the whole exercise: the call that was running was not cut.
    try testing.expect(finished_ok.load(.acquire));
    try testing.expectEqual(@as(u8, 1), clean.load(.acquire));
}

test "shutdown gives up on a call that outlasts the grace period" {
    const allocator = testing.allocator;
    var fx: ShutdownFixture = undefined;
    try fx.start(allocator);
    defer fx.deinit(allocator);
    const io = fx.threaded.io();

    var finished_ok: std.atomic.Value(bool) = .init(false);
    var calls: std.Io.Group = .init;
    try calls.concurrent(io, slowCallTask, .{ fx.client, &finished_ok });
    try std.Io.sleep(io, .fromMilliseconds(150), .awake);

    _ = fx.serving.cancel(io);
    const started = std.Io.Clock.Timestamp.now(io, .awake);
    const clean = fx.server.shutdown(100 * std.time.ns_per_ms);
    const took_ms = @divTrunc(started.durationFromNow(io).raw.nanoseconds * -1, std.time.ns_per_ms);

    try testing.expect(!clean);
    // Bounded by the grace period, not by the one-second handler.
    try testing.expect(took_ms < 700);

    // What outlasted the grace period is cut by the fixture's `deinit`.
    calls.cancel(io);
}

test "shutdown with nothing in flight returns at once" {
    const allocator = testing.allocator;
    var fx: ShutdownFixture = undefined;
    try fx.start(allocator);
    defer fx.deinit(allocator);
    const io = fx.threaded.io();

    {
        var response = try fx.client.call(echo_path, "warm", .{});
        defer response.deinit();
    }

    _ = fx.serving.cancel(io);
    const started = std.Io.Clock.Timestamp.now(io, .awake);
    try testing.expect(fx.server.shutdown(5 * std.time.ns_per_s));
    const took_ms = @divTrunc(started.durationFromNow(io).raw.nanoseconds * -1, std.time.ns_per_ms);
    try testing.expect(took_ms < 1000);
    try testing.expect(waitUnusable(fx.client, io, 1000));
}

// -- metadata ----------------------------------------------------------------

test "metadata a client sends reaches the handler" {
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

    var response = try pair.client.call(metadata_path, "", .{ .metadata = &.{
        .{ .name = "x-request-id", .value = "req-42" },
        .{ .name = "authorization", .value = "Bearer abc" },
    } });
    defer response.deinit();
    try testing.expectEqualStrings("id=req-42;auth=Bearer abc", response.message);
}

test "a call without metadata is unchanged" {
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

    var response = try pair.client.call(metadata_path, "", .{});
    defer response.deinit();
    try testing.expectEqualStrings("id=<none>;auth=<none>", response.message);
}

test "invalid metadata fails the call before anything is sent" {
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

    try testing.expectError(
        error.InvalidMetadata,
        pair.client.call(metadata_path, "", .{ .metadata = &.{.{ .name = "grpc-timeout", .value = "1n" }} }),
    );
    // And the connection is still good for the next, valid, call.
    var response = try pair.client.call(echo_path, "after", .{});
    defer response.deinit();
    try testing.expectEqualStrings("after", response.message);
}

// -- compression -------------------------------------------------------------

/// Sends one hand-built request, since the typed client cannot produce what a
/// misbehaving or merely different peer would: a `grpc-encoding` header and a
/// message whose compressed flag is set. Returns the `grpc-status` it got back.
fn rawCallStatus(
    client: *zgrpc.Client,
    authority: []const u8,
    encoding: ?[]const u8,
    compressed_flag: u8,
) !u32 {
    var fields: [8]zgrpc.Header = undefined;
    var n: usize = 0;
    for ([_]zgrpc.Header{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = echo_path },
        .{ .name = ":authority", .value = authority },
        .{ .name = "content-type", .value = "application/grpc+proto" },
        .{ .name = "te", .value = "trailers" },
    }) |h| {
        fields[n] = h;
        n += 1;
    }
    if (encoding) |e| {
        fields[n] = .{ .name = "grpc-encoding", .value = e };
        n += 1;
    }

    const stream = try client.conn.openStream(fields[0..n], false, null);
    defer client.conn.closeStream(stream);
    // Five-byte message header, then three bytes of "payload".
    try client.conn.writeData(stream, &.{ compressed_flag, 0, 0, 0, 3, 'a', 'b', 'c' }, true);
    try client.conn.flush();

    var buf: [1024]u8 = undefined;
    while (true) switch (try stream.next(&buf)) {
        .headers => |h| {
            var headers = h;
            defer headers.deinit();
            if (headers.get("grpc-status")) |text| return try std.fmt.parseInt(u32, text, 10);
        },
        .data => {},
        .end => return error.NoStatus,
    };
}

test "a request in an encoding we do not speak is UNIMPLEMENTED" {
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

    // 12 is UNIMPLEMENTED. It tells the caller the encoding is at fault, which
    // INTERNAL does not.
    try testing.expectEqual(@as(u32, 12), try rawCallStatus(pair.client, pair.authority, "gzip", 1));
}

test "identity encoding is accepted" {
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

    try testing.expectEqual(@as(u32, 0), try rawCallStatus(pair.client, pair.authority, "identity", 0));
}

test "a compressed message with no encoding named is a protocol error" {
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

    // 13 is INTERNAL: nothing says what the flag means, so nothing can be done
    // with the bytes.
    try testing.expectEqual(@as(u32, 13), try rawCallStatus(pair.client, pair.authority, null, 1));
}

// -- reset rate limit --------------------------------------------------------

test "a peer that resets streams in a flood is cut off, and others are unaffected" {
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
        .{ .reset_limit = .{ .per_second = 100, .burst = 100 } },
    );
    defer server.deinit();
    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    var serving = try io.concurrent(serveTask, .{server});
    defer _ = serving.cancel(io);

    const bystander = try zgrpc.Client.connect(allocator, io, bound, authority, .{});
    defer bystander.deinit();
    const attacker = try zgrpc.Client.connect(allocator, io, bound, authority, .{});
    defer attacker.deinit();

    const fields = [_]zgrpc.Header{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = echo_path },
        .{ .name = ":authority", .value = authority },
        .{ .name = "content-type", .value = "application/grpc+proto" },
        .{ .name = "te", .value = "trailers" },
    };

    // Open and cancel as fast as possible. The server's allowance is 100 plus
    // 100 a second, so it must give up long before this loop would.
    var opened: usize = 0;
    const limit = 50_000;
    while (opened < limit and attacker.isUsable()) : (opened += 1) {
        const stream = attacker.conn.openStream(&fields, true, null) catch break;
        stream.ended = false; // so closing it sends RST_STREAM rather than nothing
        attacker.conn.closeStream(stream);
    }
    try testing.expect(opened < limit);
    try testing.expect(!attacker.isUsable());

    // Nobody else on the server noticed.
    var response = try bystander.call(echo_path, "unaffected", .{});
    defer response.deinit();
    try testing.expectEqualStrings("unaffected", response.message);
}

test "ordinary cancellation stays well inside the default limit" {
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

    // A caller abandoning a hundred streaming calls is a bad day, not an
    // attack, and must not cost it its connection.
    for (0..100) |_| {
        var stream = try pair.client.callServerStream(count_path, "1000", .{});
        _ = try stream.next();
        stream.deinit();
    }
    var response = try pair.client.call(echo_path, "still here", .{});
    defer response.deinit();
    try testing.expectEqualStrings("still here", response.message);
}

// -- typed calls -------------------------------------------------------------

/// Stands in for a generated message. `typed` asks for a shape, not a library,
/// so a type with these two methods is all the test needs.
const Text = struct {
    body: []const u8,

    pub fn encode(
        self: Text,
        writer: *std.Io.Writer,
        allocator: std.mem.Allocator,
    ) (std.Io.Writer.Error || std.mem.Allocator.Error)!void {
        _ = allocator;
        try writer.writeAll(self.body);
    }

    pub fn decode(
        reader: *std.Io.Reader,
        allocator: std.mem.Allocator,
    ) (error{InvalidInput} || std.Io.Reader.Error || std.mem.Allocator.Error)!Text {
        const body = reader.buffered();
        if (body.len == 0) return error.InvalidInput;
        return .{ .body = try allocator.dupe(u8, body) };
    }
};

const shout_path = "/zgrpc.test.Typed/Shout";
const count_to_path = "/zgrpc.test.Typed/CountTo";

fn shout(ctx: *zgrpc.CallContext, request: Text) anyerror!Text {
    const upper = try ctx.allocator.dupe(u8, request.body);
    for (upper) |*c| c.* = std.ascii.toUpper(c.*);
    return .{ .body = upper };
}

fn countTo(ctx: *zgrpc.CallContext, request: Text, sender: *zgrpc.typed.Sender(Text)) anyerror!void {
    const n = std.fmt.parseInt(usize, request.body, 10) catch
        return ctx.fail(.invalid_argument, "not a number");
    for (0..n) |i| {
        var buf: [16]u8 = undefined;
        try sender.send(.{ .body = try std.fmt.bufPrint(&buf, "item-{d}", .{i}) });
    }
}

const typed_methods = [_]zgrpc.Method{
    zgrpc.Method.unary(shout_path, zgrpc.typed.unaryHandler(Text, Text, shout)),
    zgrpc.Method.serverStream(count_to_path, zgrpc.typed.serverStreamHandler(Text, Text, countTo)),
};

test "typed calls: unary, malformed input, and server streaming" {
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
        &typed_methods,
        .{},
    );
    defer server.deinit();

    const bound = server.boundAddress();
    const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
    defer allocator.free(authority);

    var serving = try io.concurrent(serveTask, .{server});
    defer _ = serving.cancel(io);

    const pool = try zgrpc.Pool.init(allocator, io, bound, authority, .{});
    defer pool.deinit();

    {
        var reply = try zgrpc.typed.call(Text, Text, pool, allocator, shout_path, .{ .body = "hello" }, .{});
        defer reply.deinit();
        try testing.expect(reply.status.isOk());
        try testing.expectEqualStrings("HELLO", reply.message.?.body);
    }

    // Bytes the message type cannot decode are the caller's mistake, and the
    // status says so rather than blaming the server.
    {
        var reply = try zgrpc.typed.call(Text, Text, pool, allocator, shout_path, .{ .body = "" }, .{});
        defer reply.deinit();
        try testing.expectEqual(zgrpc.Code.invalid_argument, reply.status.code);
        try testing.expect(reply.message == null);
    }

    {
        var stream = try zgrpc.typed.callServerStream(Text, Text, pool, allocator, count_to_path, .{ .body = "3" }, .{});
        defer stream.deinit();
        var seen: usize = 0;
        while (try stream.next()) |message| : (seen += 1) {
            var buf: [16]u8 = undefined;
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "item-{d}", .{seen}), message.body);
        }
        try testing.expectEqual(@as(usize, 3), seen);
        try testing.expect(stream.finalStatus().isOk());
    }

    // A handler's own failure still arrives as its status, through the stream.
    {
        var stream = try zgrpc.typed.callServerStream(Text, Text, pool, allocator, count_to_path, .{ .body = "x" }, .{});
        defer stream.deinit();
        try testing.expect((try stream.next()) == null);
        try testing.expectEqual(zgrpc.Code.invalid_argument, stream.finalStatus().code);
    }
}

// -- interceptors ------------------------------------------------------------

/// What the interceptors saw, in order. Calls in these tests are made one at a
/// time, so the order is the order they ran in.
const Trace = struct {
    entries: [16][]const u8 = undefined,
    len: std.atomic.Value(usize) = .init(0),

    fn add(self: *Trace, entry: []const u8) void {
        const i = self.len.fetchAdd(1, .acq_rel);
        self.entries[i] = entry;
    }

    fn items(self: *Trace) []const []const u8 {
        return self.entries[0..self.len.load(.acquire)];
    }
};

const Tap = struct {
    trace: *Trace,
    in: []const u8,
    out: []const u8,
    stream_in: []const u8 = "",
    stream_out: []const u8 = "",
};

fn tapUnary(context: ?*anyopaque, ctx: *zgrpc.CallContext, request: []const u8, next: zgrpc.server.UnaryNext) anyerror![]const u8 {
    const tap: *Tap = @ptrCast(@alignCast(context.?));
    tap.trace.add(tap.in);
    defer tap.trace.add(tap.out);
    return next.call(ctx, request);
}

fn tapStream(
    context: ?*anyopaque,
    ctx: *zgrpc.CallContext,
    request: []const u8,
    stream: *zgrpc.ServerStream,
    next: zgrpc.server.StreamNext,
) anyerror!void {
    const tap: *Tap = @ptrCast(@alignCast(context.?));
    tap.trace.add(tap.stream_in);
    defer tap.trace.add(tap.stream_out);
    return next.call(ctx, request, stream);
}

/// A running server with the shared test methods, and what it takes to stop it.
const Rig = struct {
    server: *zgrpc.Server,
    serving: std.Io.Future(void),
    authority: []u8,
    bound: std.Io.net.IpAddress,
    allocator: std.mem.Allocator,
    io: std.Io,

    fn start(allocator: std.mem.Allocator, io: std.Io, options: zgrpc.server.Options) !Rig {
        const server = try allocator.create(zgrpc.Server);
        errdefer allocator.destroy(server);
        server.* = try zgrpc.Server.listen(
            allocator,
            io,
            std.Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable,
            &methods,
            options,
        );
        const bound = server.boundAddress();
        const authority = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{bound.getPort()});
        errdefer allocator.free(authority);
        return .{
            .server = server,
            .serving = try io.concurrent(serveTask, .{server}),
            .authority = authority,
            .bound = bound,
            .allocator = allocator,
            .io = io,
        };
    }

    /// After the pool: it has borrowed `authority`.
    fn stop(self: *Rig) void {
        _ = self.serving.cancel(self.io);
        self.server.deinit();
        self.allocator.destroy(self.server);
        self.allocator.free(self.authority);
    }
};

test "server interceptors run outermost first, and each kind only where it is set" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var trace: Trace = .{};
    var outer: Tap = .{ .trace = &trace, .in = "outer-in", .out = "outer-out", .stream_in = "outer-stream-in", .stream_out = "outer-stream-out" };
    var inner: Tap = .{ .trace = &trace, .in = "inner-in", .out = "inner-out" };
    const chain = [_]zgrpc.ServerInterceptor{
        .{ .context = &outer, .unary = tapUnary, .server_stream = tapStream },
        // Unary only: a streaming call must pass straight through it.
        .{ .context = &inner, .unary = tapUnary },
    };

    var rig = try Rig.start(allocator, io, .{ .interceptors = &chain });
    defer rig.stop();
    const pool = try zgrpc.Pool.init(allocator, io, rig.bound, rig.authority, .{});
    defer pool.deinit();

    {
        var response = try pool.call(echo_path, "hi", .{});
        defer response.deinit();
        try testing.expectEqualStrings("hi", response.message);
    }
    try testing.expectEqualDeep(
        @as([]const []const u8, &.{ "outer-in", "inner-in", "inner-out", "outer-out" }),
        trace.items(),
    );

    trace.len.store(0, .release);
    {
        var stream = try pool.callServerStream(count_path, "2", .{});
        defer stream.deinit();
        while (try stream.next()) |_| {}
    }
    // The stream ends after the client sees it finish, so give the trailing
    // interceptor a moment: it runs when the handler returns.
    var waited: usize = 0;
    while (trace.items().len < 2 and waited < 200) : (waited += 1) try io.sleep(.fromMilliseconds(10), .awake);
    try testing.expectEqualDeep(
        @as([]const []const u8, &.{ "outer-stream-in", "outer-stream-out" }),
        trace.items(),
    );
}

/// Counts what got past it.
const Gate = struct {
    reached: std.atomic.Value(u32) = .init(0),
    token: []const u8,
};

fn authUnary(context: ?*anyopaque, ctx: *zgrpc.CallContext, request: []const u8, next: zgrpc.server.UnaryNext) anyerror![]const u8 {
    const gate: *Gate = @ptrCast(@alignCast(context.?));
    const presented = ctx.metadata("authorization") orelse return ctx.fail(.unauthenticated, "no credentials");
    if (!std.mem.eql(u8, presented, gate.token)) return ctx.fail(.permission_denied, "wrong credentials");
    _ = gate.reached.fetchAdd(1, .monotonic);
    return next.call(ctx, request);
}

test "an interceptor can refuse a call before the handler, and a client interceptor can supply the credentials" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var gate: Gate = .{ .token = "secret" };
    const server_chain = [_]zgrpc.ServerInterceptor{.{ .context = &gate, .unary = authUnary }};
    var rig = try Rig.start(allocator, io, .{ .interceptors = &server_chain });
    defer rig.stop();

    // Without credentials: refused, and the handler never ran.
    {
        const pool = try zgrpc.Pool.init(allocator, io, rig.bound, rig.authority, .{});
        defer pool.deinit();
        var response = try pool.call(echo_path, "hi", .{});
        defer response.deinit();
        try testing.expectEqual(zgrpc.Code.unauthenticated, response.call_status.code);
        try testing.expectEqual(@as(u32, 0), gate.reached.load(.monotonic));
    }

    // Wrong credentials are a different failure from none.
    {
        const pool = try zgrpc.Pool.init(allocator, io, rig.bound, rig.authority, .{});
        defer pool.deinit();
        var response = try pool.call(echo_path, "hi", .{ .metadata = &.{.{ .name = "authorization", .value = "nope" }} });
        defer response.deinit();
        try testing.expectEqual(zgrpc.Code.permission_denied, response.call_status.code);
    }

    // A client interceptor attaches the token to every call, so the caller
    // never mentions it, and sees each outcome on the way back.
    const Outcomes = struct {
        statuses: [4]zgrpc.Code = @splat(.unknown),
        count: usize = 0,

        fn attach(
            context: ?*anyopaque,
            path: []const u8,
            request: []const u8,
            options: zgrpc.client.CallOptions,
            next: zgrpc.pool.UnaryNext,
        ) anyerror!zgrpc.client.Response {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            _ = path;
            const token = [_]zgrpc.Header{.{ .name = "authorization", .value = "secret" }};
            var with_token = options;
            with_token.metadata = &token;
            const response = try next.call(request, with_token);
            self.statuses[self.count] = response.call_status.code;
            self.count += 1;
            return response;
        }
    };
    var outcomes: Outcomes = .{};
    const client_chain = [_]zgrpc.ClientInterceptor{.{ .context = &outcomes, .unary = Outcomes.attach }};
    const pool = try zgrpc.Pool.init(allocator, io, rig.bound, rig.authority, .{ .interceptors = &client_chain });
    defer pool.deinit();

    {
        var response = try pool.call(echo_path, "hi", .{});
        defer response.deinit();
        try testing.expectEqualStrings("hi", response.message);
    }
    {
        // Reaches the handler, which fails on its own terms.
        var response = try pool.call(fail_path, "", .{});
        defer response.deinit();
        try testing.expectEqual(zgrpc.Code.not_found, response.call_status.code);
    }
    try testing.expectEqual(@as(usize, 2), outcomes.count);
    try testing.expectEqual(zgrpc.Code.ok, outcomes.statuses[0]);
    try testing.expectEqual(zgrpc.Code.not_found, outcomes.statuses[1]);
    try testing.expectEqual(@as(u32, 2), gate.reached.load(.monotonic));
}

test "a client stream interceptor sees the call start, and an unknown method skips the server's chain" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var trace: Trace = .{};
    var tap: Tap = .{ .trace = &trace, .in = "server-in", .out = "server-out" };
    const server_chain = [_]zgrpc.ServerInterceptor{.{ .context = &tap, .unary = tapUnary }};
    var rig = try Rig.start(allocator, io, .{ .interceptors = &server_chain });
    defer rig.stop();

    const Started = struct {
        paths: [2][]const u8 = undefined,
        count: usize = 0,

        fn watch(
            context: ?*anyopaque,
            path: []const u8,
            request: []const u8,
            options: zgrpc.client.CallOptions,
            next: zgrpc.pool.StreamNext,
        ) anyerror!zgrpc.PooledStream {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.paths[self.count] = path;
            self.count += 1;
            return next.call(request, options);
        }
    };
    var started: Started = .{};
    const client_chain = [_]zgrpc.ClientInterceptor{.{ .context = &started, .server_stream = Started.watch }};
    const pool = try zgrpc.Pool.init(allocator, io, rig.bound, rig.authority, .{ .interceptors = &client_chain });
    defer pool.deinit();

    {
        var stream = try pool.callServerStream(count_path, "3", .{});
        defer stream.deinit();
        var seen: usize = 0;
        while (try stream.next()) |_| seen += 1;
        try testing.expectEqual(@as(usize, 3), seen);
    }
    try testing.expectEqual(@as(usize, 1), started.count);
    try testing.expectEqualStrings(count_path, started.paths[0]);

    // No such method: answered before any interceptor, which is documented
    // because an auth interceptor might otherwise be assumed to cover it.
    {
        var response = try pool.call("/zgrpc.test.Echo/Missing", "", .{});
        defer response.deinit();
        try testing.expectEqual(zgrpc.Code.unimplemented, response.call_status.code);
    }
    try testing.expectEqual(@as(usize, 0), trace.items().len);
}

// -- retries -----------------------------------------------------------------

/// Backoff short enough that a test waits milliseconds, not the defaults.
const quick_retry: zgrpc.RetryPolicy = .{ .initial_backoff_ns = std.time.ns_per_ms, .max_backoff_ns = 4 * std.time.ns_per_ms };

fn flakyFor(failures: u32) void {
    flaky_calls.store(0, .monotonic);
    flaky_failures.store(failures, .monotonic);
}

test "an idempotent call is retried past UNAVAILABLE; one that is not is left alone" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();
    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{ .retry = quick_retry });
    defer pool.deinit();

    flakyFor(2);
    {
        var response = try pool.call(flaky_path, "", .{ .idempotent = true });
        defer response.deinit();
        try testing.expectEqual(zgrpc.Code.ok, response.call_status.code);
        try testing.expectEqualStrings("ok", response.message);
    }
    try testing.expectEqual(@as(u32, 3), flaky_calls.load(.monotonic));

    // The same failures, but the caller never said running it twice is safe:
    // the server may have acted on the first, and the answer stands.
    flakyFor(2);
    {
        var response = try pool.call(flaky_path, "", .{});
        defer response.deinit();
        try testing.expectEqual(zgrpc.Code.unavailable, response.call_status.code);
    }
    try testing.expectEqual(@as(u32, 1), flaky_calls.load(.monotonic));
}

test "retries stop at max_attempts, and the last status is what comes back" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();
    var policy = quick_retry;
    policy.max_attempts = 4;
    policy.throttle = null;
    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{ .retry = policy });
    defer pool.deinit();

    flakyFor(1000);
    var response = try pool.call(flaky_path, "", .{ .idempotent = true });
    defer response.deinit();
    try testing.expectEqual(zgrpc.Code.unavailable, response.call_status.code);
    try testing.expectEqualStrings("try again", response.call_status.message);
    try testing.expectEqual(@as(u32, 4), flaky_calls.load(.monotonic));
}

test "the deadline bounds the retries and the waits between them" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();
    // A wait of up to an hour between attempts, against a 200ms deadline.
    const hour = 3600 * std.time.ns_per_s;
    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{
        .retry = .{ .initial_backoff_ns = hour, .max_backoff_ns = hour },
    });
    defer pool.deinit();

    flakyFor(1000);
    const started = std.Io.Timestamp.now(io, .awake);
    var response = try pool.call(flaky_path, "", .{ .idempotent = true, .deadline_ns = 200 * std.time.ns_per_ms });
    defer response.deinit();
    const elapsed_ms = @divTrunc(started.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds, std.time.ns_per_ms);

    // It did not wait out the backoff: it gave the first answer back.
    try testing.expectEqual(zgrpc.Code.unavailable, response.call_status.code);
    try testing.expect(elapsed_ms < 1000);
    try testing.expectEqual(@as(u32, 1), flaky_calls.load(.monotonic));
}

test "the throttle stops retrying against a server that keeps failing" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    defer fixture.stopServer();
    // Default throttle: 10 tokens, retries only above 5. Each failure costs 1.
    const pool = try zgrpc.Pool.init(allocator, io, fixture.bound, fixture.authority, .{ .retry = quick_retry });
    defer pool.deinit();

    flakyFor(1000);
    // Call 1: 10 -> 9 (retry), 8 (retry), 7, and the 3-attempt cap ends it: 3 sent.
    // Call 2: 6 (retry), 5 (not above 5, so no retry): 2 sent.
    // Call 3: 4: no retry at all: 1 sent.
    for (0..3) |_| {
        var response = try pool.call(flaky_path, "", .{ .idempotent = true });
        response.deinit();
    }
    try testing.expectEqual(@as(u32, 6), flaky_calls.load(.monotonic));
}

test "a call refused before it was sent reports Unprocessed, not a bare connection error" {
    const allocator = testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var fixture = try PoolFixture.start(allocator, io);
    defer fixture.deinit();
    const client = try zgrpc.Client.connect(allocator, io, fixture.bound, fixture.authority, .{});
    defer client.deinit();

    // One call first, so the server has accepted the connection: connecting
    // can complete before its accept loop has picked it up, and a connection
    // it never saw has no GOAWAY to receive.
    {
        var warm = try client.call(echo_path, "warm", .{});
        warm.deinit();
    }

    // Tear down the server side so the connection is told GOAWAY.
    _ = fixture.serving.cancel(io);
    try testing.expect(fixture.server.shutdown(2 * std.time.ns_per_s));
    var waited: usize = 0;
    while (client.isUsable() and waited < 300) : (waited += 1) try io.sleep(.fromMilliseconds(10), .awake);
    try testing.expect(!client.isUsable());

    try testing.expectError(error.Unprocessed, client.call(echo_path, "late", .{}));
    fixture.server.deinit();
    fixture.allocator.destroy(fixture.server);
}
