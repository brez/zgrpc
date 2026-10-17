//! Client half of the in-VPC benchmark: the milestone-2 measurement.
//!
//! Run `vpc_server` on one host and this on another, in the placement you want
//! to know about (same zone, cross-zone, a wired LAN). `examples/bench.zig` is
//! the loopback floor; this adds the network.
//!
//!   vpc_bench --target 10.0.1.5:50051 [--payload 1024] [--callers 1]
//!             [--pool 4] [--duration 30] [--warmup 5] [--rate 0]
//!             [--deadline-ms 5000] [--max-samples 200000] [--label text]
//!
//! `--rate 0` is closed loop: each caller issues its next call when the last
//! returns. That finds the latency at saturation, but it flatters the tail: a
//! stall delays every call queued behind it and the caller simply issues fewer
//! of them, so the slow period is under-sampled (coordinated omission).
//!
//! `--rate N` is open loop: N calls/s in total, each caller on a fixed
//! schedule, and a call's latency runs from when it was *due*, not from when
//! the caller got round to sending it. A stall then shows up as the many calls
//! that waited behind it. Use this one for p99/p999.

const std = @import("std");
const zgrpc = @import("zgrpc");

const path = "/zgrpc.bench.Echo/Call";

const Config = struct {
    target: []const u8 = "",
    payload: usize = 1024,
    callers: usize = 1,
    pool: usize = 4,
    duration_s: u64 = 30,
    warmup_s: u64 = 5,
    rate: u64 = 0,
    deadline_ms: u64 = 5000,
    max_samples: usize = 200_000,
    label: []const u8 = "",
};

fn usage() noreturn {
    std.debug.print(
        "usage: vpc_bench --target ip:port [--payload B] [--callers N] [--pool N]\n" ++
            "                 [--duration S] [--warmup S] [--rate CALLS_PER_S]\n" ++
            "                 [--deadline-ms MS] [--max-samples N] [--label TEXT]\n",
        .{},
    );
    std.process.exit(2);
}

fn parseArgs(args: []const [:0]const u8) Config {
    var cfg: Config = .{};
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) usage();
        const key = args[i];
        const val = args[i + 1];
        if (std.mem.eql(u8, key, "--target")) {
            cfg.target = val;
        } else if (std.mem.eql(u8, key, "--label")) {
            cfg.label = val;
        } else {
            const n = std.fmt.parseInt(u64, val, 10) catch usage();
            if (std.mem.eql(u8, key, "--payload")) {
                cfg.payload = @intCast(n);
            } else if (std.mem.eql(u8, key, "--callers")) {
                cfg.callers = @intCast(n);
            } else if (std.mem.eql(u8, key, "--pool")) {
                cfg.pool = @intCast(n);
            } else if (std.mem.eql(u8, key, "--duration")) {
                cfg.duration_s = n;
            } else if (std.mem.eql(u8, key, "--warmup")) {
                cfg.warmup_s = n;
            } else if (std.mem.eql(u8, key, "--rate")) {
                cfg.rate = n;
            } else if (std.mem.eql(u8, key, "--deadline-ms")) {
                cfg.deadline_ms = n;
            } else if (std.mem.eql(u8, key, "--max-samples")) {
                cfg.max_samples = @intCast(n);
            } else usage();
        }
    }
    if (cfg.target.len == 0 or cfg.callers == 0 or cfg.pool == 0 or cfg.duration_s == 0) usage();
    return cfg;
}

fn percentile(sorted: []const u32, p: f64) u32 {
    if (sorted.len == 0) return 0;
    const idx_f = p * @as(f64, @floatFromInt(sorted.len - 1));
    const idx: usize = @intFromFloat(@round(idx_f));
    return sorted[@min(idx, sorted.len - 1)];
}

fn us(ns: u32) f64 {
    return @as(f64, @floatFromInt(ns)) / 1000.0;
}

const Caller = struct {
    pool: *zgrpc.Pool,
    io: std.Io,
    payload: []const u8,
    /// Nanoseconds between this caller's scheduled calls; 0 means closed loop.
    interval_ns: u64,
    deadline_ns: u64,
    /// Latencies in ns, saturating at ~4.29s. Preallocated: growing it
    /// mid-run would put allocator time inside the measurement.
    samples: []u32,
    count: usize = 0,
    failures: usize = 0,
    /// Calls that returned after the deadline or not at all.
    dropped_samples: usize = 0,

    /// Calls until `end`; records only when `record` is set.
    fn run(self: *Caller, begin: std.Io.Timestamp, end: std.Io.Timestamp, record: bool) void {
        var due = begin;
        while (true) {
            if (self.interval_ns != 0) {
                const now = std.Io.Timestamp.now(self.io, .awake);
                if (due.nanoseconds > now.nanoseconds) {
                    std.Io.sleep(self.io, .fromNanoseconds(due.nanoseconds - now.nanoseconds), .awake) catch return;
                }
            }
            // Open loop measures from when the call was due. If the caller
            // is running behind, that includes the time it spent waiting.
            const start = if (self.interval_ns != 0) due else std.Io.Timestamp.now(self.io, .awake);
            if (start.nanoseconds >= end.nanoseconds) return;

            if (self.pool.call(path, self.payload, .{ .deadline_ns = self.deadline_ns })) |*r| {
                var response = r.*;
                response.deinit();
                const done = std.Io.Timestamp.now(self.io, .awake);
                if (record) self.add(@intCast(done.nanoseconds - start.nanoseconds));
            } else |_| {
                if (record) self.failures += 1;
            }
            if (self.interval_ns != 0) due.nanoseconds += self.interval_ns;
        }
    }

    fn add(self: *Caller, ns: u64) void {
        if (self.count >= self.samples.len) {
            self.dropped_samples += 1;
            return;
        }
        self.samples[self.count] = @intCast(@min(ns, std.math.maxInt(u32)));
        self.count += 1;
    }
};

fn runPhase(workers: []Caller, io: std.Io, seconds: u64, record: bool) !u64 {
    const begin = std.Io.Timestamp.now(io, .awake);
    const end: std.Io.Timestamp = .{ .nanoseconds = begin.nanoseconds + @as(i96, seconds) * std.time.ns_per_s };
    var group: std.Io.Group = .init;
    errdefer group.cancel(io);
    for (workers) |*w| try group.concurrent(io, Caller.run, .{ w, begin, end, record });
    try group.await(io);
    const done = std.Io.Timestamp.now(io, .awake);
    return @intCast(done.nanoseconds - begin.nanoseconds);
}

pub fn main(init: std.process.Init.Minimal) !void {
    // Not DebugAllocator: see examples/bench.zig.
    const allocator = std.heap.smp_allocator;

    const args = try init.args.toSlice(allocator);
    const cfg = parseArgs(args);

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var out_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(io, &out_buf);
    const out = &stdout.interface;

    const colon = std.mem.lastIndexOfScalar(u8, cfg.target, ':') orelse usage();
    const port = std.fmt.parseInt(u16, cfg.target[colon + 1 ..], 10) catch usage();
    const address = std.Io.net.IpAddress.parse(cfg.target[0..colon], port) catch usage();

    const pool = try zgrpc.Pool.init(allocator, io, address, cfg.target, .{ .size = cfg.pool });
    defer pool.deinit();

    const payload = try allocator.alloc(u8, cfg.payload);
    for (payload, 0..) |*b, i| b.* = @intCast(i % 251);

    // Per-caller schedule: callers share the total rate evenly.
    const interval_ns: u64 = if (cfg.rate == 0) 0 else @max(1, cfg.callers * std.time.ns_per_s / cfg.rate);

    const per_caller = @min(cfg.max_samples, if (cfg.rate == 0)
        cfg.max_samples
    else
        cfg.duration_s * std.time.ns_per_s / interval_ns + 16);
    const all_samples = try allocator.alloc(u32, cfg.callers * per_caller);

    const workers = try allocator.alloc(Caller, cfg.callers);
    for (workers, 0..) |*w, i| {
        w.* = .{
            .pool = pool,
            .io = io,
            .payload = payload,
            .interval_ns = interval_ns,
            .deadline_ns = cfg.deadline_ms * std.time.ns_per_ms,
            .samples = all_samples[i * per_caller ..][0..per_caller],
        };
    }

    try out.print("# target {s}  payload {d} B  callers {d}  pool {d}  {s}  duration {d}s (warmup {d}s)  {s}\n", .{
        cfg.target,
        cfg.payload,
        cfg.callers,
        cfg.pool,
        if (cfg.rate == 0) "closed loop" else "open loop",
        cfg.duration_s,
        cfg.warmup_s,
        cfg.label,
    });
    try out.flush();

    // Warmup opens every connection the run will use and fills HPACK tables;
    // none of it is recorded.
    if (cfg.warmup_s > 0) _ = try runPhase(workers, io, cfg.warmup_s, false);

    const elapsed_ns = try runPhase(workers, io, cfg.duration_s, true);

    var total: usize = 0;
    var failures: usize = 0;
    var dropped: usize = 0;
    for (workers) |w| {
        total += w.count;
        failures += w.failures;
        dropped += w.dropped_samples;
    }

    // Gather the per-caller slices into one contiguous run for sorting.
    const merged = try allocator.alloc(u32, total);
    var at: usize = 0;
    for (workers) |w| {
        @memcpy(merged[at..][0..w.count], w.samples[0..w.count]);
        at += w.count;
    }
    std.mem.sort(u32, merged, {}, std.sort.asc(u32));

    var sum: u128 = 0;
    for (merged) |s| sum += s;
    const mean_ns: u32 = if (total == 0) 0 else @intCast(sum / total);
    const per_second = @as(f64, @floatFromInt(total + failures)) * 1e9 / @as(f64, @floatFromInt(elapsed_ns));

    try out.print(
        "n={d}  min {d:.1}us  mean {d:.1}us  p50 {d:.1}us  p90 {d:.1}us  p99 {d:.1}us  p999 {d:.1}us  p9999 {d:.1}us  max {d:.1}us  {d:.0} calls/s  failures {d}",
        .{
            total,
            us(if (total == 0) 0 else merged[0]),
            us(mean_ns),
            us(percentile(merged, 0.50)),
            us(percentile(merged, 0.90)),
            us(percentile(merged, 0.99)),
            us(percentile(merged, 0.999)),
            us(percentile(merged, 0.9999)),
            us(if (total == 0) 0 else merged[total - 1]),
            per_second,
            failures,
        },
    );
    if (dropped > 0) try out.print("  UNRECORDED {d} (raise --max-samples)", .{dropped});
    try out.writeAll("\n");
    try out.flush();

    if (failures > 0) std.process.exit(1);
}
