//! gRPC server over h2c: unary and server-streaming methods.
//!
//! Concurrency: connections are served one at a time on the calling thread.
//! This is deliberate for now — the HPACK decoder in the upstream `http2.zig`
//! dependency resets a process-wide scratch buffer on every header field, so
//! decoding two header blocks concurrently would corrupt both. Moving to a
//! thread per connection requires either a per-connection HPACK decoder
//! upstream or a lock around decoding; see README "Known constraints".

const std = @import("std");
const h2 = @import("h2.zig");
const framing = @import("framing.zig");
const status = @import("status.zig");
const timeout = @import("timeout.zig");
const tcp = @import("tcp.zig");

const Status = status.Status;
const Code = status.Code;

/// Everything a handler learns about the call in progress.
pub const CallContext = struct {
    /// Per-call arena. Anything the handler allocates from this — including
    /// the response it returns — is freed when the call completes, so handlers
    /// never need their own cleanup.
    allocator: std.mem.Allocator,
    /// Full method path, e.g. `/helloworld.Greeter/SayHello`.
    path: []const u8,
    /// Client deadline in nanoseconds from call start, if `grpc-timeout` was sent.
    deadline_ns: ?u64,
    request_headers: *const h2.Headers,

    /// Status returned to the client. A handler that wants to fail with a
    /// specific code sets this and returns an empty body.
    status: Status = Status.ok,

    pub fn fail(self: *CallContext, code: Code, message: []const u8) error{HandlerFailed} {
        self.status = .{ .code = code, .message = message };
        return error.HandlerFailed;
    }
};

/// A unary method implementation: request bytes in, response bytes out.
///
/// Bytes rather than generated types on purpose — codegen sits *above* this
/// and produces functions of this shape, so the transport never needs to know
/// about protobuf message types.
pub const MethodHandler = *const fn (ctx: *CallContext, request: []const u8) anyerror![]const u8;

/// A server-streaming method implementation: one request in, any number of
/// responses out via `stream.send`.
///
/// Returning normally ends the stream with the status on `ctx` (OK unless the
/// handler set otherwise).
pub const StreamHandler = *const fn (
    ctx: *CallContext,
    request: []const u8,
    stream: *ServerStream,
) anyerror!void;

pub const Handler = union(enum) {
    unary: MethodHandler,
    server_stream: StreamHandler,
};

pub const Method = struct {
    /// Fully-qualified path: `/package.Service/Method`.
    path: []const u8,
    handler: Handler,

    pub fn unary(path: []const u8, handler: MethodHandler) Method {
        return .{ .path = path, .handler = .{ .unary = handler } };
    }

    pub fn serverStream(path: []const u8, handler: StreamHandler) Method {
        return .{ .path = path, .handler = .{ .server_stream = handler } };
    }
};

/// The handle a server-streaming handler uses to emit messages.
///
/// Messages go out as they are produced. Nothing accumulates a full response
/// here, which is the entire point: a range query that materialises every row
/// before the client sees the first one is not streaming, whatever the wire
/// format says.
pub const ServerStream = struct {
    allocator: std.mem.Allocator,
    conn: *h2.Connection,
    stream_id: u32,
    /// Response HEADERS are written lazily on the first `send`, so a handler
    /// that fails before producing anything can still be answered with a
    /// compact Trailers-Only response.
    headers_sent: bool = false,
    /// Reused across sends so streaming N messages does not mean N allocations.
    frame_buf: std.ArrayList(u8) = .empty,

    fn deinit(self: *ServerStream) void {
        self.frame_buf.deinit(self.allocator);
    }

    /// Sends one message. `message` is not retained.
    ///
    /// Returns `error.StreamReset` once the client has cancelled, which
    /// unwinds the handler and stops it producing. Handlers that loop over a
    /// large result set get this for free from `try stream.send(...)`.
    pub fn send(self: *ServerStream, message: []const u8) !void {
        // Cancellation is noticed when the write path blocks for flow-control
        // credit and reads the pending RST_STREAM. See "Known constraints" in
        // the README for why detection is bounded by the peer's window rather
        // than immediate.
        if (self.conn.isStreamReset(self.stream_id)) return error.StreamReset;

        if (!self.headers_sent) {
            try self.conn.writeHeaders(self.stream_id, &.{
                .{ .name = ":status", .value = "200" },
                .{ .name = "content-type", .value = "application/grpc+proto" },
            }, false);
            self.headers_sent = true;
        }

        self.frame_buf.clearRetainingCapacity();
        const needed = framing.header_len + message.len;
        var writer = std.Io.Writer.fixed(
            try self.frame_buf.addManyAsSlice(self.allocator, needed),
        );
        try framing.writeMessage(&writer, message);

        try self.conn.writeData(self.stream_id, self.frame_buf.items, false);
        // Flushed per message on purpose. Letting messages sit until the 64KB
        // write buffer happens to fill would make delivery depend on message
        // size rather than on the handler producing them, which defeats
        // streaming for exactly the small-message case that needs it.
        try self.conn.flush();
    }
};

pub const Options = struct {
    max_message_bytes: u32 = framing.default_max_message_bytes,
    /// Size of the socket read/write buffers. One HTTP/2 frame plus headers
    /// fits comfortably in the default.
    io_buffer_bytes: usize = 64 * 1024,
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    methods: []const Method,
    options: Options,
    net_server: std.Io.net.Server,

    pub fn listen(
        allocator: std.mem.Allocator,
        io: std.Io,
        address: std.Io.net.IpAddress,
        methods: []const Method,
        options: Options,
    ) !Server {
        var addr = address;
        const net_server = try addr.listen(io, .{ .reuse_address = true });
        return .{
            .allocator = allocator,
            .io = io,
            .methods = methods,
            .options = options,
            .net_server = net_server,
        };
    }

    pub fn deinit(self: *Server) void {
        self.net_server.deinit(self.io);
    }

    /// The address actually bound, with the ephemeral port resolved — the
    /// caller needs this when listening on port 0.
    pub fn boundAddress(self: *const Server) std.Io.net.IpAddress {
        return self.net_server.socket.address;
    }

    /// Accepts one connection and serves calls on it until the peer closes it
    /// or sends GOAWAY.
    pub fn acceptAndServe(self: *Server) !void {
        const stream = try self.net_server.accept(self.io);
        defer stream.close(self.io);
        try self.serveConnection(stream);
    }

    pub fn serveConnection(self: *Server, stream: std.Io.net.Stream) !void {
        try tcp.setNoDelay(stream);

        const read_buf = try self.allocator.alloc(u8, self.options.io_buffer_bytes);
        defer self.allocator.free(read_buf);
        const write_buf = try self.allocator.alloc(u8, self.options.io_buffer_bytes);
        defer self.allocator.free(write_buf);

        var reader = stream.reader(self.io, read_buf);
        var writer = stream.writer(self.io, write_buf);

        var conn = h2.Connection.init(self.allocator, &reader.interface, &writer.interface, true);
        defer conn.deinit();

        try conn.readClientPreface();
        try conn.sendSettings();
        try conn.flush();

        while (true) {
            self.serveOneCall(&conn) catch |err| switch (err) {
                // A peer that hangs up between calls is the normal way a
                // connection ends, not an error worth propagating.
                error.EndOfStream, error.ConnectionClosed, error.ConnectionResetByPeer => return,
                else => return err,
            };
            if (conn.goaway_received != null) return;
        }
    }

    // `anyerror` so the disconnect cases below can be named explicitly;
    // the inferred set varies with the Io implementation in use.
    fn serveOneCall(self: *Server, conn: *h2.Connection) anyerror!void {
        // Wait for the HEADERS frame that starts the next call.
        const first = while (true) {
            const frame = try conn.readFrame();
            switch (frame.header.frame_type) {
                .HEADERS => break frame,
                .GOAWAY => return,
                .RST_STREAM => continue,
                else => continue,
            }
        };

        const stream_id = first.header.stream_id;
        defer conn.releaseStream(stream_id);
        var headers = try conn.readHeaderBlock(first);
        defer headers.deinit();

        var call_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer call_arena.deinit();

        const path = headers.get(":path") orelse {
            try self.respondStatusOnly(conn, stream_id, .{
                .code = .internal,
                .message = "missing :path",
            });
            return;
        };

        const deadline_ns: ?u64 = if (headers.get("grpc-timeout")) |t|
            timeout.decode(t) catch null
        else
            null;

        // Collect the request body. END_STREAM on the HEADERS frame means a
        // zero-length request, which is legal.
        var decoder = framing.Decoder.init(self.allocator);
        decoder.max_message_bytes = self.options.max_message_bytes;
        defer decoder.deinit();

        var request_bytes: []const u8 = "";
        if (!first.header.flags.has(h2.FrameFlags.END_STREAM)) {
            request_bytes = self.readRequestMessage(conn, stream_id, &decoder, call_arena.allocator()) catch |err| {
                try self.respondStatusOnly(conn, stream_id, .{
                    .code = if (err == error.MessageTooLarge) .resource_exhausted else .internal,
                    .message = @errorName(err),
                });
                return;
            };
        }

        const handler = self.lookup(path) orelse {
            try self.respondStatusOnly(conn, stream_id, .{
                .code = .unimplemented,
                .message = "method not found",
            });
            return;
        };

        var ctx: CallContext = .{
            .allocator = call_arena.allocator(),
            .path = path,
            .deadline_ns = deadline_ns,
            .request_headers = &headers,
        };

        switch (handler) {
            .unary => |call| {
                const response = call(&ctx, request_bytes) catch |err| {
                    try self.respondStatusOnly(conn, stream_id, failureStatus(&ctx, err));
                    return;
                };

                if (!ctx.status.isOk()) {
                    try self.respondStatusOnly(conn, stream_id, ctx.status);
                    return;
                }

                try self.respond(conn, stream_id, response, Status.ok);
            },

            .server_stream => |call| {
                var stream: ServerStream = .{
                    .allocator = self.allocator,
                    .conn = conn,
                    .stream_id = stream_id,
                };
                defer stream.deinit();

                if (call(&ctx, request_bytes, &stream)) |_| {
                    try self.finishStream(conn, stream_id, &stream, ctx.status);
                } else |err| {
                    // A client that cancelled has already torn the stream down.
                    // There is nothing to report to and nothing to report — it
                    // is a normal end to the call, not a failure.
                    if (err == error.StreamReset or conn.isStreamReset(stream_id)) return;

                    // Once any message has gone out the response headers are
                    // already on the wire, so the failure has to be reported in
                    // trailers rather than as a Trailers-Only response.
                    try self.finishStream(conn, stream_id, &stream, failureStatus(&ctx, err));
                }
            },
        }
    }

    fn finishStream(
        self: *Server,
        conn: *h2.Connection,
        stream_id: u32,
        stream: *ServerStream,
        call_status: Status,
    ) !void {
        if (!stream.headers_sent) {
            // A stream that produced nothing collapses to Trailers-Only, which
            // is legal for both an empty success and an early failure.
            try self.respondStatusOnly(conn, stream_id, call_status);
            return;
        }
        try self.writeTrailers(conn, stream_id, call_status);
        try conn.flush();
    }

    fn readRequestMessage(
        self: *Server,
        conn: *h2.Connection,
        stream_id: u32,
        decoder: *framing.Decoder,
        arena: std.mem.Allocator,
    ) ![]const u8 {
        _ = self;
        while (true) {
            const frame = try conn.readFrame();
            const mine = frame.header.stream_id == stream_id;

            switch (frame.header.frame_type) {
                .DATA => {
                    if (!mine) continue;
                    try decoder.push(h2.dataPayload(frame));
                    if (try decoder.next()) |msg| {
                        // Copy out: the decoder's buffer is reused, and the
                        // handler may hold this for the whole call.
                        return try arena.dupe(u8, msg.payload);
                    }
                    if (frame.header.flags.has(h2.FrameFlags.END_STREAM)) {
                        if (decoder.isDrained()) return "";
                        return error.TruncatedMessage;
                    }
                },
                // Trailing HEADERS from the client ends the request stream.
                // Decoded unconditionally: HPACK's dynamic table is
                // connection-wide, so a skipped block corrupts later ones.
                .HEADERS => {
                    var headers = try conn.readHeaderBlock(frame);
                    headers.deinit();
                    if (mine and frame.header.flags.has(h2.FrameFlags.END_STREAM)) return "";
                },
                .RST_STREAM => if (mine) return error.StreamReset,
                .GOAWAY => return error.ConnectionClosed,
                else => {},
            }
        }
    }

    /// A handler that set an explicit status keeps it; anything else is an
    /// unexpected failure and maps to INTERNAL.
    fn failureStatus(ctx: *const CallContext, err: anyerror) Status {
        if (ctx.status.isOk()) return .{ .code = .internal, .message = @errorName(err) };
        return ctx.status;
    }

    fn lookup(self: *Server, path: []const u8) ?Handler {
        for (self.methods) |m| {
            if (std.mem.eql(u8, m.path, path)) return m.handler;
        }
        return null;
    }

    /// Full unary response: headers, one message, then trailers.
    fn respond(
        self: *Server,
        conn: *h2.Connection,
        stream_id: u32,
        message: []const u8,
        call_status: Status,
    ) !void {
        try conn.writeHeaders(stream_id, &.{
            .{ .name = ":status", .value = "200" },
            .{ .name = "content-type", .value = "application/grpc+proto" },
        }, false);

        // Frame the message into a scratch buffer so the DATA frame is written
        // in one piece.
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.allocator);
        const needed = framing.header_len + message.len;
        try body.ensureTotalCapacity(self.allocator, needed);
        var body_writer = std.Io.Writer.fixed(try body.addManyAsSlice(self.allocator, needed));
        try framing.writeMessage(&body_writer, message);

        try conn.writeData(stream_id, body.items, false);
        try self.writeTrailers(conn, stream_id, call_status);
        try conn.flush();
    }

    /// A failed call still returns HTTP 200 — gRPC carries its own status in
    /// trailers, and a non-200 would be read as a transport failure instead.
    ///
    /// Sends headers and trailers in a single "Trailers-Only" header block,
    /// which is what the spec prescribes for a response with no message.
    fn respondStatusOnly(
        self: *Server,
        conn: *h2.Connection,
        stream_id: u32,
        call_status: Status,
    ) !void {
        var buf: [16]u8 = undefined;
        const code_text = try std.fmt.bufPrint(&buf, "{d}", .{call_status.code.toInt()});

        const encoded_message = try status.encodeMessage(self.allocator, call_status.message);
        defer self.allocator.free(encoded_message);

        var fields: [4]h2.Header = undefined;
        var n: usize = 0;
        fields[n] = .{ .name = ":status", .value = "200" };
        n += 1;
        fields[n] = .{ .name = "content-type", .value = "application/grpc+proto" };
        n += 1;
        fields[n] = .{ .name = "grpc-status", .value = code_text };
        n += 1;
        if (encoded_message.len > 0) {
            fields[n] = .{ .name = "grpc-message", .value = encoded_message };
            n += 1;
        }

        try conn.writeHeaders(stream_id, fields[0..n], true);
        try conn.flush();
    }

    fn writeTrailers(
        self: *Server,
        conn: *h2.Connection,
        stream_id: u32,
        call_status: Status,
    ) !void {
        var buf: [16]u8 = undefined;
        const code_text = try std.fmt.bufPrint(&buf, "{d}", .{call_status.code.toInt()});

        const encoded_message = try status.encodeMessage(self.allocator, call_status.message);
        defer self.allocator.free(encoded_message);

        var fields: [2]h2.Header = undefined;
        var n: usize = 0;
        fields[n] = .{ .name = "grpc-status", .value = code_text };
        n += 1;
        if (encoded_message.len > 0) {
            fields[n] = .{ .name = "grpc-message", .value = encoded_message };
            n += 1;
        }

        try conn.writeHeaders(stream_id, fields[0..n], true);
    }
};
