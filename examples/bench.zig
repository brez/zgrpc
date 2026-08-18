//! Loopback latency baseline for unary calls.
//!
//! This is NOT the milestone-2 benchmark. It runs client and server on one
//! machine over loopback, so it measures the library's own overhead with the
//! network removed. Its purpose is to establish what zgrpc costs before RTT is
//! added, so that when the real in-VPC numbers come in, a bad tail can be
//! attributed to the transport rather than guessed at.
//!
//! Same-AZ RTT alone typically consumes a large share of the ~5ms budget; treat
//! the numbers here as a floor, not a prediction.

const std = @import("std");
const zgrpc = @import("zgrpc");

const path = "/zgrpc.bench.Echo/Call";

fn echo(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    return ctx.allocator.dupe(u8, request);
}

const methods = [_]zgrpc.Method{zgrpc.Method.unary(path, echo)};

fn serveThread(server: *zgrpc.Server) void {
    server.acceptAndServe() catch {};
}

fn serveTask(server: *zgrpc.Server) void {
    server.serve() catch {};
}

fn percentile(sorted: []const u64, p: f64) u64 {
    if (sorted.len == 0) return 0;
    const idx_f = p * @as(f64, @floatFromInt(sorted.len - 1));
    const idx: usize = @intFromFloat(@round(idx_f));
    return sorted[@min(idx, sorted.len - 1)];
}

fn runOne(
    allocator: std.mem.Allocator,
    io: std.Io,
    out: *std.Io.Writer,
    payload_size: usize,
    iterations: usize,
) !void {
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

    const thread = try std.Thread.spawn(.{}, serveThread, .{&server});

    const client = try zgrpc.Client.connect(allocator, io, bound, authority, .{});

    const payload = try allocator.alloc(u8, payload_size);
    defer allocator.free(payload);
    for (payload, 0..) |*b, i| b.* = @intCast(i % 251);

    const samples = try allocator.alloc(u64, iterations);
    defer allocator.free(samples);

    // Warm up so HPACK dynamic-table state and allocator growth are not
    // charged to the first measured samples.
    for (0..100) |_| {
        var r = try client.call(path, payload, .{});
        r.deinit();
    }

    for (samples) |*sample| {
        // `.awake` is CLOCK_MONOTONIC on Linux: wall-clock jumps would
        // corrupt tail percentiles.
        const start = std.Io.Timestamp.now(io, .awake);
        var response = try client.call(path, payload, .{});
        const end = std.Io.Timestamp.now(io, .awake);
        sample.* = @intCast(end.nanoseconds - start.nanoseconds);
        std.debug.assert(response.message.len == payload_size);
        response.deinit();
    }

    client.deinit();
    thread.join();

    std.mem.sort(u64, samples, {}, std.sort.asc(u64));

    var total: u128 = 0;
    for (samples) |s| total += s;
    const mean = @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(samples.len));

    try out.print("{d:>7} B  n={d:<6}  mean {d:>7.1}us  p50 {d:>7.1}us  p99 {d:>8.1}us  p999 {d:>8.1}us  max {d:>8.1}us\n", .{
        payload_size,
        iterations,
        mean / 1000.0,
        @as(f64, @floatFromInt(percentile(samples, 0.50))) / 1000.0,
        @as(f64, @floatFromInt(percentile(samples, 0.99))) / 1000.0,
        @as(f64, @floatFromInt(percentile(samples, 0.999))) / 1000.0,
        @as(f64, @floatFromInt(samples[samples.len - 1])) / 1000.0,
    });
}

/// One caller's worth of a concurrent run.
const Caller = struct {
    pool: *zgrpc.Pool,
    io: std.Io,
    payload: []const u8,
    samples: []u64,
    failures: usize = 0,

    fn run(self: *Caller) void {
        for (self.samples) |*sample| {
            const start = std.Io.Timestamp.now(self.io, .awake);
            var response = self.pool.call(path, self.payload, .{}) catch {
                self.failures += 1;
                sample.* = 0;
                continue;
            };
            const end = std.Io.Timestamp.now(self.io, .awake);
            response.deinit();
            sample.* = @intCast(end.nanoseconds - start.nanoseconds);
        }
    }
};

/// Latency under load, through a pool.
///
/// This is what the pool exists for and the only way to size one: a single
/// connection multiplexes happily until its reader task saturates a core or
/// the peer's stream limit bites, and the point at which that happens is a
/// property of the machine, not something to guess at. Compare the same caller
/// count across pool sizes.
fn runConcurrent(
    allocator: std.mem.Allocator,
    io: std.Io,
    out: *std.Io.Writer,
    payload_size: usize,
    callers: usize,
    pool_size: usize,
    per_caller: usize,
) !void {
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

    var serving = try io.concurrent(serveTask, .{&server});
    defer _ = serving.cancel(io);

    const payload = try allocator.alloc(u8, payload_size);
    defer allocator.free(payload);
    for (payload, 0..) |*b, i| b.* = @intCast(i % 251);

    const pool = try zgrpc.Pool.init(allocator, io, bound, authority, .{ .size = pool_size });
    defer pool.deinit();

    const workers = try allocator.alloc(Caller, callers);
    defer allocator.free(workers);
    const all_samples = try allocator.alloc(u64, callers * per_caller);
    defer allocator.free(all_samples);

    for (workers, 0..) |*worker, i| {
        worker.* = .{
            .pool = pool,
            .io = io,
            .payload = payload,
            .samples = all_samples[i * per_caller ..][0..per_caller],
        };
    }

    // Warm up so pool growth and HPACK table state are not charged to the
    // measurement, and so every connection the run will use already exists.
    {
        var warm: std.Io.Group = .init;
        errdefer warm.cancel(io);
        for (workers) |*worker| {
            var probe: Caller = .{
                .pool = pool,
                .io = io,
                .payload = payload,
                .samples = worker.samples[0..@min(worker.samples.len, 20)],
            };
            try warm.concurrent(io, Caller.run, .{&probe});
        }
        try warm.await(io);
    }

    const started = std.Io.Timestamp.now(io, .awake);
    {
        var group: std.Io.Group = .init;
        errdefer group.cancel(io);
        for (workers) |*worker| try group.concurrent(io, Caller.run, .{worker});
        try group.await(io);
    }
    const finished = std.Io.Timestamp.now(io, .awake);

    var failures: usize = 0;
    for (workers) |worker| failures += worker.failures;

    const elapsed_ns: u64 = @intCast(finished.nanoseconds - started.nanoseconds);
    const calls = callers * per_caller;
    const per_second = @as(f64, @floatFromInt(calls)) * 1e9 / @as(f64, @floatFromInt(elapsed_ns));

    std.mem.sort(u64, all_samples, {}, std.sort.asc(u64));

    try out.print(
        "{d:>7} B  callers {d:<4} pool {d}   p50 {d:>7.1}us  p99 {d:>8.1}us  p999 {d:>8.1}us  {d:>9.0} calls/s{s}\n",
        .{
            payload_size,
            callers,
            pool_size,
            @as(f64, @floatFromInt(percentile(all_samples, 0.50))) / 1000.0,
            @as(f64, @floatFromInt(percentile(all_samples, 0.99))) / 1000.0,
            @as(f64, @floatFromInt(percentile(all_samples, 0.999))) / 1000.0,
            per_second,
            if (failures > 0) "  (FAILURES)" else "",
        },
    );
}

pub fn main() !void {
    // Deliberately not DebugAllocator: it captures a stack trace on every
    // allocation, which on ARM costs more than the RPC being measured and
    // makes the numbers say more about the allocator than about zgrpc.
    const allocator = std.heap.smp_allocator;

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var out_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(io, &out_buf);
    const out = &stdout.interface;

    try out.writeAll("zgrpc unary latency over loopback (no network RTT)\n\n");

    const sizes = [_]usize{ 64, 1024, 16 * 1024, 128 * 1024 };
    for (sizes) |size| {
        const iterations: usize = if (size >= 128 * 1024) 2_000 else 10_000;
        try runOne(allocator, io, out, size, iterations);
        try out.flush();
    }

    try out.writeAll("\nunder load, through a pool (same payload, varying callers and pool size)\n\n");

    const loads = [_]struct { callers: usize, pool: usize }{
        .{ .callers = 8, .pool = 1 },
        .{ .callers = 8, .pool = 4 },
        .{ .callers = 64, .pool = 1 },
        .{ .callers = 64, .pool = 4 },
    };
    for (loads) |load| {
        try runConcurrent(allocator, io, out, 1024, load.callers, load.pool, 500);
        try out.flush();
    }

    try out.writeAll("\nLoopback only. The milestone-2 target is p50/p99/p999 in-VPC.\n");
    try out.flush();
}
