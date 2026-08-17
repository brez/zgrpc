//! Socket tuning for latency-sensitive RPC.

const std = @import("std");

/// Disables Nagle's algorithm.
///
/// This is not a micro-optimisation; leaving it on is measurably fatal to the
/// tail. gRPC is a request/response ping-pong of small writes, which is the
/// exact pattern where Nagle interacts with the peer's delayed ACK: the sender
/// holds a small segment back waiting for an ACK that the receiver is holding
/// back waiting for more data. On Linux that standoff resolves only when the
/// 40ms delayed-ACK timer fires.
///
/// Measured on loopback before this call was added, 64-byte unary calls ran
/// p50 95us but p999 48ms — a 500x tail, sitting right on the delayed-ACK
/// timer. Every production gRPC implementation sets this for the same reason.
pub fn setNoDelay(stream: std.Io.net.Stream) !void {
    const enable: c_int = 1;
    try std.posix.setsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        std.mem.asBytes(&enable),
    );
}
