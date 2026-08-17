//! Unary gRPC client over h2c.
//!
//! The connection is long-lived by design: gRPC's whole latency argument rests
//! on reusing one HTTP/2 connection across calls, so nothing here reconnects
//! per request. `call` may be invoked repeatedly on the same `Client`.

const std = @import("std");
const h2 = @import("h2.zig");
const framing = @import("framing.zig");
const status = @import("status.zig");
const timeout = @import("timeout.zig");
const tcp = @import("tcp.zig");

const Status = status.Status;
const Code = status.Code;

pub const Options = struct {
    max_message_bytes: u32 = framing.default_max_message_bytes,
    io_buffer_bytes: usize = 64 * 1024,
    /// Sent as `user-agent`; some servers log or gate on it.
    user_agent: []const u8 = "grpc-zig/0.1",
};

pub const CallOptions = struct {
    /// Propagated to the server as `grpc-timeout`.
    deadline_ns: ?u64 = null,
};

/// The result of a unary call.
///
/// `message` and `status.message` are owned by `arena`; call `deinit` to
/// release them.
pub const Response = struct {
    arena: std.heap.ArenaAllocator,
    message: []const u8,
    call_status: Status,

    pub fn deinit(self: *Response) void {
        self.arena.deinit();
    }
};

/// An in-progress server-streaming call.
///
/// Messages are yielded as they arrive off the connection; nothing accumulates
/// the full response. That is the point of the type — a range query over a
/// large window must not require the client to hold every row at once.
///
/// The stream borrows its `Client` and holds the connection for the duration of
/// the call, so it must be drained (or deinited) before another call is made on
/// the same client.
pub const ResponseStream = struct {
    client: *Client,
    stream_id: u32,
    decoder: framing.Decoder,
    /// Owns only the final status message, not the yielded payloads.
    arena: std.heap.ArenaAllocator,

    call_status: ?Status = null,
    /// Set once END_STREAM has been seen; no more frames belong to this call.
    ended: bool = false,

    /// Ends the call. A stream abandoned before the server finished is
    /// cancelled with RST_STREAM, which is what tells the peer to stop
    /// producing — a caller that reads the first few rows of a long range
    /// query and walks away should not leave the server streaming the rest
    /// into a connection nobody is reading.
    ///
    /// Errors are swallowed: deinit runs on cleanup paths where there is
    /// nothing useful to do with a failure, and the connection is already
    /// suspect if this cannot be written.
    pub fn deinit(self: *ResponseStream) void {
        if (!self.ended) {
            self.client.conn.sendRstStream(self.stream_id, h2.error_code_cancel) catch {};
            self.client.conn.flush() catch {};
        }
        self.client.conn.releaseStream(self.stream_id);
        self.decoder.deinit();
        self.arena.deinit();
    }

    /// Returns the next message, or null once the server has finished.
    ///
    /// The returned slice borrows the decoder's buffer and is invalidated by
    /// the following `next` call. A caller that needs to retain a message past
    /// that point must copy it — which is the trade that keeps this from
    /// allocating per message.
    pub fn next(self: *ResponseStream) !?[]const u8 {
        while (true) {
            if (try self.decoder.next()) |msg| {
                if (msg.compressed) return error.CompressionNotSupported;
                return msg.payload;
            }

            if (self.ended) {
                // A stream that ends mid-message was truncated; reporting it as
                // a clean end would silently lose data.
                if (!self.decoder.isDrained()) return error.TruncatedMessage;
                return null;
            }

            try self.pump();
        }
    }

    /// Reads one frame belonging to this call and applies it.
    fn pump(self: *ResponseStream) !void {
        const conn = &self.client.conn;
        const frame = try conn.readFrame();
        const mine = frame.header.stream_id == self.stream_id;

        switch (frame.header.frame_type) {
            .HEADERS => {
                // Decoded even when the block belongs to another stream.
                // HPACK's dynamic table is connection-wide state built by
                // every header block in order, so skipping one leaves the
                // decoder permanently out of step with the peer's encoder and
                // corrupts every later block on the connection.
                var headers = try conn.readHeaderBlock(frame);
                defer headers.deinit();
                if (!mine) return;

                if (headers.get("grpc-status")) |code_text| {
                    const code = try Code.fromTrailer(code_text);
                    const raw = headers.get("grpc-message") orelse "";
                    self.call_status = .{
                        .code = code,
                        .message = try status.decodeMessage(self.arena.allocator(), raw),
                    };
                } else if (headers.get(":status")) |http_status| {
                    // A non-200 means the request never reached gRPC at all.
                    if (!std.mem.eql(u8, http_status, "200")) {
                        self.call_status = .{
                            .code = .unavailable,
                            .message = try self.arena.allocator().dupe(u8, http_status),
                        };
                    }
                }

                if (frame.header.flags.has(h2.FrameFlags.END_STREAM)) self.ended = true;
            },
            .DATA => {
                if (!mine) return;
                try self.decoder.push(h2.dataPayload(frame));
                if (frame.header.flags.has(h2.FrameFlags.END_STREAM)) self.ended = true;
            },
            .RST_STREAM => if (mine) return error.StreamReset,
            .GOAWAY => return error.ConnectionClosed,
            else => {},
        }
    }

    /// The call's final status. Only meaningful once `next` has returned null.
    ///
    /// A stream that ended without `grpc-status` is a protocol violation, and
    /// is reported as such rather than being assumed successful.
    pub fn finalStatus(self: *const ResponseStream) Status {
        return self.call_status orelse .{
            .code = .internal,
            .message = "missing grpc-status trailer",
        };
    }
};

pub const Client = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    conn: h2.Connection,
    options: Options,

    /// `:authority` for outgoing requests, e.g. "127.0.0.1:50051".
    authority: []const u8,

    read_buf: []u8,
    write_buf: []u8,
    reader: *std.Io.net.Stream.Reader,
    writer: *std.Io.net.Stream.Writer,

    /// Connects and completes the HTTP/2 handshake.
    ///
    /// `authority` is borrowed and must outlive the client.
    pub fn connect(
        allocator: std.mem.Allocator,
        io: std.Io,
        address: std.Io.net.IpAddress,
        authority: []const u8,
        options: Options,
    ) !*Client {
        var addr = address;
        const stream = try addr.connect(io, .{ .mode = .stream });
        errdefer stream.close(io);
        try tcp.setNoDelay(stream);

        // Heap-allocated because the h2.Connection stores pointers to the
        // reader/writer interfaces, which must not move.
        const self = try allocator.create(Client);
        errdefer allocator.destroy(self);

        const read_buf = try allocator.alloc(u8, options.io_buffer_bytes);
        errdefer allocator.free(read_buf);
        const write_buf = try allocator.alloc(u8, options.io_buffer_bytes);
        errdefer allocator.free(write_buf);

        const reader = try allocator.create(std.Io.net.Stream.Reader);
        errdefer allocator.destroy(reader);
        const writer = try allocator.create(std.Io.net.Stream.Writer);
        errdefer allocator.destroy(writer);

        reader.* = stream.reader(io, read_buf);
        writer.* = stream.writer(io, write_buf);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .stream = stream,
            .conn = h2.Connection.init(allocator, &reader.interface, &writer.interface, false),
            .options = options,
            .authority = authority,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .reader = reader,
            .writer = writer,
        };

        try self.conn.sendClientPreface();
        try self.conn.flush();
        return self;
    }

    pub fn deinit(self: *Client) void {
        self.conn.sendGoAway(0, h2.error_code_no_error) catch {};
        self.conn.flush() catch {};
        self.conn.deinit();
        self.stream.close(self.io);
        self.allocator.destroy(self.reader);
        self.allocator.destroy(self.writer);
        self.allocator.free(self.read_buf);
        self.allocator.free(self.write_buf);
        self.allocator.destroy(self);
    }

    /// Performs one unary call: send a single message, receive a single
    /// message plus status.
    pub fn call(
        self: *Client,
        path: []const u8,
        request: []const u8,
        call_options: CallOptions,
    ) !Response {
        const stream_id = try self.sendRequest(path, request, call_options);
        defer self.conn.releaseStream(stream_id);
        return self.readResponse(stream_id);
    }

    /// Starts a server-streaming call: send a single message, then read
    /// responses one at a time via `ResponseStream.next`.
    ///
    /// The request half is identical to a unary call — server streaming is a
    /// property of the response, not of the request — so the difference lives
    /// entirely in how the response is consumed.
    pub fn callServerStream(
        self: *Client,
        path: []const u8,
        request: []const u8,
        call_options: CallOptions,
    ) !ResponseStream {
        const stream_id = try self.sendRequest(path, request, call_options);
        return .{
            .client = self,
            .stream_id = stream_id,
            .decoder = decoder: {
                var d = framing.Decoder.init(self.allocator);
                d.max_message_bytes = self.options.max_message_bytes;
                break :decoder d;
            },
            .arena = std.heap.ArenaAllocator.init(self.allocator),
        };
    }

    /// Writes request HEADERS plus the single request message, and returns the
    /// stream id the response will arrive on.
    fn sendRequest(
        self: *Client,
        path: []const u8,
        request: []const u8,
        call_options: CallOptions,
    ) !u32 {
        const stream_id = self.conn.openStream();

        var timeout_buf: [16]u8 = undefined;
        const timeout_text: ?[]const u8 = if (call_options.deadline_ns) |ns|
            try timeout.encode(&timeout_buf, ns)
        else
            null;

        var fields: [8]h2.Header = undefined;
        var n: usize = 0;
        // Pseudo-headers must come first and in this order.
        fields[n] = .{ .name = ":method", .value = "POST" };
        n += 1;
        fields[n] = .{ .name = ":scheme", .value = "http" };
        n += 1;
        fields[n] = .{ .name = ":path", .value = path };
        n += 1;
        fields[n] = .{ .name = ":authority", .value = self.authority };
        n += 1;
        fields[n] = .{ .name = "content-type", .value = "application/grpc+proto" };
        n += 1;
        fields[n] = .{ .name = "te", .value = "trailers" };
        n += 1;
        fields[n] = .{ .name = "user-agent", .value = self.options.user_agent };
        n += 1;
        if (timeout_text) |t| {
            fields[n] = .{ .name = "grpc-timeout", .value = t };
            n += 1;
        }

        try self.conn.writeHeaders(stream_id, fields[0..n], false);

        // Frame the request into one buffer so headers and body leave in as
        // few writes as possible.
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(self.allocator);
        const needed = framing.header_len + request.len;
        var body_writer = std.Io.Writer.fixed(try body.addManyAsSlice(self.allocator, needed));
        try framing.writeMessage(&body_writer, request);

        try self.conn.writeData(stream_id, body.items, true);
        try self.conn.flush();

        return stream_id;
    }

    fn readResponse(self: *Client, stream_id: u32) !Response {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const arena_alloc = arena.allocator();

        var decoder = framing.Decoder.init(self.allocator);
        decoder.max_message_bytes = self.options.max_message_bytes;
        defer decoder.deinit();

        var message: []const u8 = "";
        var got_message = false;
        var call_status: ?Status = null;

        while (true) {
            const frame = try self.conn.readFrame();
            const mine = frame.header.stream_id == stream_id;

            switch (frame.header.frame_type) {
                .HEADERS => {
                    // Decoded even for other streams: HPACK's dynamic table is
                    // connection-wide, so skipping a block desynchronizes the
                    // decoder from the peer's encoder for good.
                    var headers = try self.conn.readHeaderBlock(frame);
                    defer headers.deinit();
                    if (!mine) continue;

                    // A header block carrying grpc-status is the trailer block
                    // — or, for an empty response, a Trailers-Only block that
                    // is simultaneously the first and last.
                    if (headers.get("grpc-status")) |code_text| {
                        const code = try Code.fromTrailer(code_text);
                        const raw_message = headers.get("grpc-message") orelse "";
                        const decoded = try status.decodeMessage(arena_alloc, raw_message);
                        call_status = .{ .code = code, .message = decoded };
                    } else {
                        // A non-200 means the request never reached gRPC at all.
                        if (headers.get(":status")) |http_status| {
                            if (!std.mem.eql(u8, http_status, "200")) {
                                call_status = .{
                                    .code = .unavailable,
                                    .message = try arena_alloc.dupe(u8, http_status),
                                };
                            }
                        }
                    }

                    if (frame.header.flags.has(h2.FrameFlags.END_STREAM)) break;
                },
                .DATA => {
                    if (!mine) continue;
                    try decoder.push(h2.dataPayload(frame));
                    while (try decoder.next()) |msg| {
                        if (msg.compressed) return error.CompressionNotSupported;
                        message = try arena_alloc.dupe(u8, msg.payload);
                        got_message = true;
                    }
                    if (frame.header.flags.has(h2.FrameFlags.END_STREAM)) break;
                },
                .RST_STREAM => if (mine) return error.StreamReset,
                .GOAWAY => return error.ConnectionClosed,
                else => {},
            }
        }

        if (!got_message and call_status == null) return error.MissingStatus;

        return .{
            .arena = arena,
            .message = message,
            // A response that ends without grpc-status is a protocol
            // violation; report it as such rather than inventing success.
            .call_status = call_status orelse .{
                .code = .internal,
                .message = "missing grpc-status trailer",
            },
        };
    }
};
