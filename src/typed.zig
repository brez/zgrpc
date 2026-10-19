//! Typed calls over message types that encode and decode themselves.
//!
//! The rest of zgrpc moves `[]const u8` and never learns what is in it. This
//! file is the one place that does, and only by shape: a message type `T` is
//! anything with
//!
//!     fn encode(self: T, writer: *std.Io.Writer, allocator: Allocator) !void
//!     fn decode(reader: *std.Io.Reader, allocator: Allocator) !T
//!
//! which is what zig-protobuf generates. Nothing here imports it, so zgrpc
//! still has no protobuf dependency, and the generated stubs (`gen/`) are thin
//! because the logic they would otherwise repeat is here, where a test with a
//! fake message type can reach it.
//!
//! Decode errors other than `OutOfMemory` are the sender's fault. On the
//! server they become INVALID_ARGUMENT rather than INTERNAL, so a caller
//! sending garbage is told so.

const std = @import("std");
const client = @import("client.zig");
const pool_mod = @import("pool.zig");
const server = @import("server.zig");
const Status = @import("status.zig").Status;

/// What a unary call returns. `message` is present exactly when `status` is OK,
/// and everything in it, including `status.message`, lives until `deinit`.
pub fn Reply(comptime T: type) type {
    return struct {
        arena: std.heap.ArenaAllocator,
        status: Status,
        message: ?T,

        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
        }
    };
}

fn encodeToBytes(allocator: std.mem.Allocator, message: anytype) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    try message.encode(&out.writer, allocator);
    return out.written();
}

fn decodeFrom(comptime T: type, allocator: std.mem.Allocator, bytes: []const u8) !T {
    var reader = std.Io.Reader.fixed(bytes);
    return T.decode(&reader, allocator);
}

fn decodeRequest(comptime Req: type, ctx: *server.CallContext, bytes: []const u8) !Req {
    return decodeFrom(Req, ctx.allocator, bytes) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return ctx.fail(.invalid_argument, "malformed request message"),
    };
}

// Server side.

/// Adapts `fn (ctx: *CallContext, request: Req) anyerror!Resp` to the bytes
/// handler the server takes. Everything is allocated from `ctx.allocator`, so
/// neither the request nor the response needs freeing.
pub fn unaryHandler(
    comptime Req: type,
    comptime Resp: type,
    comptime handler: anytype,
) server.MethodHandler {
    return struct {
        fn call(ctx: *server.CallContext, request: []const u8) anyerror![]const u8 {
            const req = try decodeRequest(Req, ctx, request);
            const reply: Resp = try handler(ctx, req);
            return encodeToBytes(ctx.allocator, reply);
        }
    }.call;
}

/// Where a server-streaming handler puts its messages.
pub fn Sender(comptime Resp: type) type {
    return struct {
        raw: *server.ServerStream,
        /// Reset on every `send`, so streaming N messages does not mean N
        /// encode buffers held until the call ends.
        scratch: std.heap.ArenaAllocator,

        const Self = @This();

        fn init(raw: *server.ServerStream) Self {
            return .{ .raw = raw, .scratch = .init(raw.allocator) };
        }

        fn deinit(self: *Self) void {
            self.scratch.deinit();
        }

        /// Fails with `error.StreamReset` or `error.DeadlineExceeded` once the
        /// client has cancelled or the deadline has passed, like
        /// `ServerStream.send`; `try` it and the handler stops producing.
        pub fn send(self: *Self, message: Resp) !void {
            _ = self.scratch.reset(.retain_capacity);
            try self.raw.send(try encodeToBytes(self.scratch.allocator(), message));
        }
    };
}

/// Adapts `fn (ctx, request: Req, sender: *Sender(Resp)) anyerror!void`.
pub fn serverStreamHandler(
    comptime Req: type,
    comptime Resp: type,
    comptime handler: anytype,
) server.StreamHandler {
    return struct {
        fn call(ctx: *server.CallContext, request: []const u8, raw: *server.ServerStream) anyerror!void {
            const req = try decodeRequest(Req, ctx, request);
            var sender: Sender(Resp) = .init(raw);
            defer sender.deinit();
            try handler(ctx, req, &sender);
        }
    }.call;
}

// Client side.

/// One unary call. The request is encoded into a scratch arena that is gone
/// before this returns; the reply owns its own.
pub fn call(
    comptime Req: type,
    comptime Resp: type,
    pool: *pool_mod.Pool,
    allocator: std.mem.Allocator,
    path: []const u8,
    request: Req,
    options: client.CallOptions,
) !Reply(Resp) {
    var scratch: std.heap.ArenaAllocator = .init(allocator);
    defer scratch.deinit();

    var response = try pool.call(path, try encodeToBytes(scratch.allocator(), request), options);
    errdefer response.deinit();

    if (!response.call_status.isOk()) {
        return .{ .arena = response.arena, .status = response.call_status, .message = null };
    }

    const message = decodeFrom(Resp, response.arena.allocator(), response.message) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.MalformedResponse,
    };
    return .{ .arena = response.arena, .status = response.call_status, .message = message };
}

/// The messages of a server-streaming call, decoded as they arrive.
pub fn Receiver(comptime Resp: type) type {
    return struct {
        stream: pool_mod.PooledStream,
        arena: std.heap.ArenaAllocator,

        const Self = @This();

        /// Cancels the call if it is not finished, and releases the connection.
        pub fn deinit(self: *Self) void {
            self.stream.deinit();
            self.arena.deinit();
        }

        /// The next message, or null once the server has finished; then
        /// `finalStatus` says how. Valid until the next call to `next`, the
        /// same rule as the byte stream underneath: copy what must outlive it.
        pub fn next(self: *Self) !?Resp {
            const bytes = (try self.stream.next()) orelse return null;
            _ = self.arena.reset(.retain_capacity);
            return decodeFrom(Resp, self.arena.allocator(), bytes) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.MalformedResponse,
            };
        }

        pub fn finalStatus(self: *const Self) Status {
            return self.stream.finalStatus();
        }
    };
}

pub fn callServerStream(
    comptime Req: type,
    comptime Resp: type,
    pool: *pool_mod.Pool,
    allocator: std.mem.Allocator,
    path: []const u8,
    request: Req,
    options: client.CallOptions,
) !Receiver(Resp) {
    var scratch: std.heap.ArenaAllocator = .init(allocator);
    defer scratch.deinit();

    const stream = try pool.callServerStream(path, try encodeToBytes(scratch.allocator(), request), options);
    return .{ .stream = stream, .arena = .init(allocator) };
}
