//! zgrpc — gRPC over real HTTP/2 (h2c) for Zig.
//!
//! Layers, bottom to top:
//!   h2.zig      HTTP/2 connection driver (frames + HPACK from `http2.zig`)
//!   framing.zig gRPC 5-byte length-prefixed message framing
//!   status.zig  grpc-status / grpc-message
//!   timeout.zig grpc-timeout deadline propagation
//!   client.zig  unary + server-streaming client, one connection
//!   server.zig  unary + server-streaming server
//!   pool.zig    several persistent connections to one peer
//!   typed.zig   typed calls over message types with encode/decode methods

const std = @import("std");

pub const h2 = @import("h2.zig");
pub const tcp = @import("tcp.zig");
pub const framing = @import("framing.zig");
pub const status = @import("status.zig");
pub const timeout = @import("timeout.zig");
pub const client = @import("client.zig");
pub const server = @import("server.zig");
pub const pool = @import("pool.zig");
pub const typed = @import("typed.zig");
pub const retry = @import("retry.zig");

pub const Keepalive = h2.Keepalive;
pub const Header = h2.Header;
pub const Code = status.Code;
pub const Status = status.Status;
pub const Client = client.Client;
pub const Pool = pool.Pool;
pub const PooledStream = pool.PooledStream;
pub const ClientInterceptor = pool.Interceptor;
pub const RetryPolicy = retry.Policy;
pub const Server = server.Server;
pub const Method = server.Method;
pub const ServerStream = server.ServerStream;
pub const StreamHandler = server.StreamHandler;
pub const ResponseStream = client.ResponseStream;
pub const CallContext = server.CallContext;
pub const MethodHandler = server.MethodHandler;
pub const ServerInterceptor = server.Interceptor;
pub const Reply = typed.Reply;

test {
    _ = h2;
    _ = tcp;
    _ = framing;
    _ = status;
    _ = timeout;
    _ = client;
    _ = server;
    _ = pool;
    _ = typed;
    _ = retry;
}
