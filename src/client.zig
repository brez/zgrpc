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

        defer self.conn.releaseStream(stream_id);
        return self.readResponse(stream_id);
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
            if (frame.header.stream_id != stream_id and frame.header.frame_type != .GOAWAY) continue;

            switch (frame.header.frame_type) {
                .HEADERS => {
                    var headers = try self.conn.readHeaderBlock(frame);
                    defer headers.deinit();

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
                    try decoder.push(h2.dataPayload(frame));
                    while (try decoder.next()) |msg| {
                        if (msg.compressed) return error.CompressionNotSupported;
                        message = try arena_alloc.dupe(u8, msg.payload);
                        got_message = true;
                    }
                    if (frame.header.flags.has(h2.FrameFlags.END_STREAM)) break;
                },
                .RST_STREAM => return error.StreamReset,
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
