//! Echo server for the in-VPC benchmark (`examples/vpc_bench.zig`).
//!
//! Unlike `echo_server.zig` it binds an address you choose, so a client on
//! another host can reach it, and it does nothing else: no failure paths, no
//! streaming, no allocator that captures stack traces. What it measures is the
//! library plus the network, not the handler.
//!
//!   vpc_server [--listen 0.0.0.0:50051]

const std = @import("std");
const zgrpc = @import("zgrpc");

const path = "/zgrpc.bench.Echo/Call";

fn echo(ctx: *zgrpc.CallContext, request: []const u8) anyerror![]const u8 {
    return ctx.allocator.dupe(u8, request);
}

const methods = [_]zgrpc.Method{zgrpc.Method.unary(path, echo)};

pub fn main(init: std.process.Init.Minimal) !void {
    const allocator = std.heap.smp_allocator;

    var listen: []const u8 = "0.0.0.0:50051";
    const args = try init.args.toSlice(allocator);
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--listen") and i + 1 < args.len) {
            i += 1;
            listen = args[i];
        } else {
            std.debug.print("usage: vpc_server [--listen host:port]\n", .{});
            std.process.exit(2);
        }
    }

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const colon = std.mem.lastIndexOfScalar(u8, listen, ':') orelse return error.InvalidListenAddress;
    const port = try std.fmt.parseInt(u16, listen[colon + 1 ..], 10);
    const address = try std.Io.net.IpAddress.parse(listen[0..colon], port);

    var server = try zgrpc.Server.listen(allocator, io, address, &methods, .{});
    defer server.deinit();

    var out_buf: [64]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(io, &out_buf);
    try stdout.interface.print("listening {s}\n", .{listen});
    try stdout.interface.flush();

    try server.serve();
}
