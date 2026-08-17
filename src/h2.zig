//! A minimal HTTP/2 (h2c) connection driver, built on the frame codec and
//! HPACK implementation from `http2.zig` rather than on its high-level server.
//!
//! Why not use that library's request/response API: gRPC needs a frame
//! sequence that an HTTP-shaped API cannot express. A unary response is
//!
//!     HEADERS (:status, content-type)      -- no END_STREAM
//!     DATA    (length-prefixed message)    -- no END_STREAM
//!     HEADERS (grpc-status, grpc-message)  -- END_STREAM
//!
//! That trailing HEADERS frame is where `grpc-status` lives, and it must arrive
//! *after* the body. The upstream connection state machine sends response
//! headers at most once (`if (stream.response_headers_sent) return;`) and puts
//! END_STREAM on the final DATA frame, so trailers are unreachable through it.
//! This module therefore drives frames directly, and reuses upstream only for
//! the parts that are genuinely hard and stable: framing and HPACK.

const std = @import("std");
const http2 = @import("http2");

const Hpack = http2.Hpack;
const FrameHeader = http2.FrameHeader;
pub const FrameType = http2.FrameType;
pub const FrameFlags = http2.FrameFlags;

/// The client connection preface, sent before any frame. Its bytes are fixed
/// by RFC 9113 and act as a guard against non-HTTP/2 peers.
pub const client_preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub const default_max_frame_size: u32 = 16_384;
pub const default_initial_window: i64 = 65_535;

/// SETTINGS parameter identifiers (RFC 9113 §6.5.2).
const setting_header_table_size: u16 = 0x1;
const setting_enable_push: u16 = 0x2;
const setting_max_concurrent_streams: u16 = 0x3;
const setting_initial_window_size: u16 = 0x4;
const setting_max_frame_size: u16 = 0x5;
const setting_max_header_list_size: u16 = 0x6;

pub const error_code_no_error: u32 = 0x0;
pub const error_code_protocol_error: u32 = 0x1;
pub const error_code_internal_error: u32 = 0x2;
pub const error_code_flow_control_error: u32 = 0x3;
pub const error_code_frame_size_error: u32 = 0x6;
pub const error_code_cancel: u32 = 0x8;

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

/// A decoded header block. Names and values are owned by `arena`, because
/// upstream's HPACK decoder returns views into a buffer it resets on every
/// field — see `decodeHeaderBlock`.
pub const Headers = struct {
    arena: std.heap.ArenaAllocator,
    list: std.ArrayList(Header) = .empty,

    pub fn init(allocator: std.mem.Allocator) Headers {
        return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *Headers) void {
        self.arena.deinit();
    }

    pub fn get(self: *const Headers, name: []const u8) ?[]const u8 {
        for (self.list.items) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

pub const Frame = struct {
    header: FrameHeader,
    payload: []const u8,
};

pub const GoAway = struct {
    last_stream_id: u32,
    error_code: u32,
};

pub const Connection = struct {
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    is_server: bool,

    /// Separate HPACK contexts per direction: each endpoint keeps its own
    /// encoder table, and they must not be shared.
    encode_table: Hpack.DynamicTable,
    decode_table: Hpack.DynamicTable,

    /// Odd for clients, even for servers (RFC 9113 §5.1.1).
    next_stream_id: u32,

    peer_max_frame_size: u32 = default_max_frame_size,
    peer_initial_window: i64 = default_initial_window,

    /// Connection-level flow-control window for data we send.
    send_window: i64 = default_initial_window,
    /// Per-stream send windows. HTTP/2 flow control is enforced at *both*
    /// levels, and a peer that has connection budget left may still have
    /// closed an individual stream's window — ignoring these stalls forever.
    stream_windows: std.AutoHashMapUnmanaged(u32, i64) = .empty,
    /// Bytes received since we last told the peer to open the window back up.
    unacked_recv: u32 = 0,

    goaway_received: ?GoAway = null,

    /// Scratch buffer reused for encoding header blocks, so a steady stream of
    /// calls does not allocate per request.
    header_buf: std.ArrayList(u8) = .empty,
    /// Scratch buffer for reading frame payloads.
    payload_buf: std.ArrayList(u8) = .empty,

    pub fn init(
        allocator: std.mem.Allocator,
        reader: *std.Io.Reader,
        writer: *std.Io.Writer,
        is_server: bool,
    ) Connection {
        return .{
            .allocator = allocator,
            .reader = reader,
            .writer = writer,
            .is_server = is_server,
            .encode_table = Hpack.DynamicTable.init(allocator, 4096),
            .decode_table = Hpack.DynamicTable.init(allocator, 4096),
            .next_stream_id = if (is_server) 2 else 1,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.encode_table.deinit();
        self.decode_table.deinit();
        self.header_buf.deinit(self.allocator);
        self.payload_buf.deinit(self.allocator);
        self.stream_windows.deinit(self.allocator);
    }

    /// Drops per-stream state once a call has completed. Without this the
    /// window map grows for the life of a long-lived connection.
    pub fn releaseStream(self: *Connection, stream_id: u32) void {
        _ = self.stream_windows.remove(stream_id);
    }

    /// Send window for `stream_id`, created at the peer's advertised initial
    /// size the first time the stream is written to.
    fn streamWindow(self: *Connection, stream_id: u32) !*i64 {
        const entry = try self.stream_windows.getOrPut(self.allocator, stream_id);
        if (!entry.found_existing) entry.value_ptr.* = self.peer_initial_window;
        return entry.value_ptr;
    }

    /// Allocates the next stream id for a locally-initiated stream.
    pub fn openStream(self: *Connection) u32 {
        const id = self.next_stream_id;
        self.next_stream_id += 2;
        return id;
    }

    // -- handshake ---------------------------------------------------------

    /// Client side: preface bytes followed by our SETTINGS.
    pub fn sendClientPreface(self: *Connection) !void {
        try self.writer.writeAll(client_preface);
        try self.sendSettings();
    }

    /// Server side: validate the client preface before anything else, so a
    /// plain HTTP/1 or garbage connection fails immediately and clearly.
    pub fn readClientPreface(self: *Connection) !void {
        var buf: [client_preface.len]u8 = undefined;
        try self.reader.readSliceAll(&buf);
        if (!std.mem.eql(u8, &buf, client_preface)) return error.InvalidPreface;
    }

    pub fn sendSettings(self: *Connection) !void {
        // Advertise no server push: gRPC never uses it, and refusing up front
        // avoids having to handle PUSH_PROMISE at all.
        const params = [_]struct { id: u16, value: u32 }{
            .{ .id = setting_enable_push, .value = 0 },
            .{ .id = setting_max_frame_size, .value = default_max_frame_size },
            .{ .id = setting_initial_window_size, .value = @intCast(default_initial_window) },
        };

        var payload: [params.len * 6]u8 = undefined;
        for (params, 0..) |p, i| {
            std.mem.writeInt(u16, payload[i * 6 ..][0..2], p.id, .big);
            std.mem.writeInt(u32, payload[i * 6 + 2 ..][0..4], p.value, .big);
        }
        try self.writeFrame(.SETTINGS, 0, 0, &payload);
    }

    fn sendSettingsAck(self: *Connection) !void {
        try self.writeFrame(.SETTINGS, FrameFlags.ACK, 0, "");
    }

    // -- frame writing -----------------------------------------------------

    fn writeFrame(
        self: *Connection,
        frame_type: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        var header = FrameHeader{
            .length = @intCast(payload.len),
            .frame_type = frame_type,
            .flags = FrameFlags.init(flags),
            .reserved = false,
            .stream_id = stream_id,
        };
        try header.write(self.writer);
        if (payload.len > 0) try self.writer.writeAll(payload);
    }

    /// Writes a HEADERS frame, splitting into CONTINUATION frames when the
    /// encoded block exceeds the peer's max frame size.
    ///
    /// This is used for request headers, response headers *and* trailers —
    /// trailers are simply a header block with END_STREAM and no pseudo-headers.
    pub fn writeHeaders(
        self: *Connection,
        stream_id: u32,
        headers: []const Header,
        end_stream: bool,
    ) !void {
        self.header_buf.clearRetainingCapacity();
        for (headers) |h| {
            try Hpack.encodeHeaderField(
                .{ .name = h.name, .value = h.value },
                &self.encode_table,
                &self.header_buf,
                self.allocator,
            );
        }

        const block = self.header_buf.items;
        const max = self.peer_max_frame_size;
        const first_len = @min(block.len, max);
        const fits = first_len == block.len;

        var flags: u8 = 0;
        if (end_stream) flags |= FrameFlags.END_STREAM;
        if (fits) flags |= FrameFlags.END_HEADERS;
        try self.writeFrame(.HEADERS, flags, stream_id, block[0..first_len]);

        var offset = first_len;
        while (offset < block.len) {
            const chunk_len = @min(block.len - offset, max);
            const last = offset + chunk_len == block.len;
            try self.writeFrame(
                .CONTINUATION,
                if (last) FrameFlags.END_HEADERS else 0,
                stream_id,
                block[offset..][0..chunk_len],
            );
            offset += chunk_len;
        }
    }

    /// Writes DATA, respecting both the peer's max frame size and the
    /// connection-level flow-control window.
    pub fn writeData(
        self: *Connection,
        stream_id: u32,
        data: []const u8,
        end_stream: bool,
    ) !void {
        if (data.len == 0) {
            try self.writeFrame(
                .DATA,
                if (end_stream) FrameFlags.END_STREAM else 0,
                stream_id,
                "",
            );
            return;
        }

        var offset: usize = 0;
        while (offset < data.len) {
            // Block until both the connection and this stream have budget.
            // Anything already buffered must reach the peer first, or it will
            // never send the WINDOW_UPDATE being waited for — that deadlock is
            // exactly what an unflushed writer causes here.
            while (self.send_window <= 0 or (try self.streamWindow(stream_id)).* <= 0) {
                try self.flush();
                // `pumpOnce`, not `readFrame`: a WINDOW_UPDATE is precisely the
                // frame being waited for, and `readFrame` would swallow it and
                // keep blocking for a stream frame that cannot arrive.
                _ = try self.pumpOnce();
                if (self.goaway_received != null) return error.ConnectionClosed;
            }

            const available = @min(self.send_window, (try self.streamWindow(stream_id)).*);
            const window_limit: usize = @intCast(@max(available, 0));
            const chunk_len = @min(
                @min(data.len - offset, self.peer_max_frame_size),
                window_limit,
            );
            const last = offset + chunk_len == data.len;
            try self.writeFrame(
                .DATA,
                if (last and end_stream) FrameFlags.END_STREAM else 0,
                stream_id,
                data[offset..][0..chunk_len],
            );
            self.send_window -= @intCast(chunk_len);
            (try self.streamWindow(stream_id)).* -= @intCast(chunk_len);
            offset += chunk_len;
        }
    }

    pub fn sendRstStream(self: *Connection, stream_id: u32, code: u32) !void {
        var payload: [4]u8 = undefined;
        std.mem.writeInt(u32, &payload, code, .big);
        try self.writeFrame(.RST_STREAM, 0, stream_id, &payload);
    }

    pub fn sendGoAway(self: *Connection, last_stream_id: u32, code: u32) !void {
        var payload: [8]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], last_stream_id, .big);
        std.mem.writeInt(u32, payload[4..8], code, .big);
        try self.writeFrame(.GOAWAY, 0, 0, &payload);
    }

    fn sendWindowUpdate(self: *Connection, stream_id: u32, increment: u32) !void {
        var payload: [4]u8 = undefined;
        std.mem.writeInt(u32, &payload, increment, .big);
        try self.writeFrame(.WINDOW_UPDATE, 0, stream_id, &payload);
    }

    pub fn flush(self: *Connection) !void {
        try self.writer.flush();
    }

    // -- frame reading -----------------------------------------------------

    /// Reads until a frame the caller cares about arrives, handling
    /// connection-management frames along the way.
    ///
    /// The returned payload is valid until the next read.
    pub fn readFrame(self: *Connection) !Frame {
        while (true) {
            if (try self.pumpOnce()) |frame| return frame;
        }
    }

    /// Reads exactly one frame. Returns null if it was a connection-management
    /// frame that was handled internally.
    ///
    /// The distinction from `readFrame` matters: a sender blocked on a closed
    /// flow-control window must regain control the moment a WINDOW_UPDATE is
    /// processed. Looping internally until a stream frame arrives would
    /// deadlock, since the peer is waiting for the very data being withheld.
    pub fn pumpOnce(self: *Connection) !?Frame {
        const header = try FrameHeader.read(self.reader);
        if (header.length > 0) {
            try self.payload_buf.resize(self.allocator, header.length);
            try self.reader.readSliceAll(self.payload_buf.items);
        } else {
            self.payload_buf.clearRetainingCapacity();
        }
        const payload = self.payload_buf.items;

        switch (header.frame_type) {
            .SETTINGS => {
                if (!header.flags.has(FrameFlags.ACK)) {
                    try self.applySettings(payload);
                    try self.sendSettingsAck();
                    try self.flush();
                }
                return null;
            },
            .PING => {
                if (!header.flags.has(FrameFlags.ACK)) {
                    try self.writeFrame(.PING, FrameFlags.ACK, 0, payload);
                    try self.flush();
                }
                return null;
            },
            .WINDOW_UPDATE => {
                if (payload.len != 4) return error.InvalidFrameSize;
                const increment = std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff;
                if (increment == 0) return error.ProtocolError;
                if (header.stream_id == 0) {
                    self.send_window += increment;
                } else if (self.stream_windows.getPtr(header.stream_id)) |window| {
                    window.* += increment;
                }
                return null;
            },
            .GOAWAY => {
                if (payload.len < 8) return error.InvalidFrameSize;
                self.goaway_received = .{
                    .last_stream_id = std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff,
                    .error_code = std.mem.readInt(u32, payload[4..8], .big),
                };
                return Frame{ .header = header, .payload = payload };
            },
            .DATA => {
                // Reopen the receive window eagerly. Waiting until the
                // window is nearly exhausted would stall the peer mid-call
                // and show up directly in tail latency.
                self.unacked_recv += header.length;
                if (self.unacked_recv >= default_initial_window / 2) {
                    try self.sendWindowUpdate(0, self.unacked_recv);
                    try self.sendWindowUpdate(header.stream_id, self.unacked_recv);
                    try self.flush();
                    self.unacked_recv = 0;
                }
                return Frame{ .header = header, .payload = payload };
            },
            else => return Frame{ .header = header, .payload = payload },
        }
    }

    fn applySettings(self: *Connection, payload: []const u8) !void {
        if (payload.len % 6 != 0) return error.InvalidFrameSize;
        var i: usize = 0;
        while (i < payload.len) : (i += 6) {
            const id = std.mem.readInt(u16, payload[i..][0..2], .big);
            const value = std.mem.readInt(u32, payload[i + 2 ..][0..4], .big);
            switch (id) {
                setting_max_frame_size => {
                    if (value < 16_384 or value > 16_777_215) return error.ProtocolError;
                    self.peer_max_frame_size = value;
                },
                setting_initial_window_size => {
                    if (value > 0x7fff_ffff) return error.FlowControlError;
                    // A change applies retroactively to every open stream by
                    // the delta, not just to streams opened afterwards
                    // (RFC 9113 §6.9.2).
                    const delta = @as(i64, value) - self.peer_initial_window;
                    self.peer_initial_window = value;
                    var it = self.stream_windows.valueIterator();
                    while (it.next()) |window| window.* += delta;
                },
                setting_header_table_size => try self.encode_table.updateMaxSize(value),
                // Ignored: we never push, and the remaining parameters do not
                // change how this connection behaves.
                setting_enable_push, setting_max_concurrent_streams, setting_max_header_list_size => {},
                else => {},
            }
        }
    }

    /// Reads a complete header block, following CONTINUATION frames, and
    /// decodes it.
    ///
    /// `first` must be the HEADERS frame already read from the wire.
    pub fn readHeaderBlock(self: *Connection, first: Frame) !Headers {
        // Copy the first fragment out: reading CONTINUATION frames reuses
        // `payload_buf` and would otherwise clobber it.
        var block: std.ArrayList(u8) = .empty;
        defer block.deinit(self.allocator);
        try block.appendSlice(self.allocator, stripPadding(first));

        var end_headers = first.header.flags.has(FrameFlags.END_HEADERS);
        while (!end_headers) {
            const next = try self.readFrame();
            if (next.header.frame_type != .CONTINUATION) return error.ExpectedContinuation;
            if (next.header.stream_id != first.header.stream_id) return error.ProtocolError;
            try block.appendSlice(self.allocator, next.payload);
            end_headers = next.header.flags.has(FrameFlags.END_HEADERS);
        }

        return self.decodeHeaderBlock(block.items);
    }

    /// Decodes an HPACK block into owned headers.
    ///
    /// Upstream's `decodeHeaderFieldView` resets a process-wide scratch buffer
    /// on entry and returns slices that may point into it, so each field is
    /// duplicated into the arena before the next one is decoded. Doing this
    /// lazily would silently corrupt every header but the last.
    fn decodeHeaderBlock(self: *Connection, block: []const u8) !Headers {
        var headers = Headers.init(self.allocator);
        errdefer headers.deinit();
        const arena = headers.arena.allocator();

        var offset: usize = 0;
        while (offset < block.len) {
            const decoded = try Hpack.decodeHeaderFieldView(block[offset..], &self.decode_table);
            if (decoded.bytes_consumed == 0) return error.InvalidEncoding;
            offset += decoded.bytes_consumed;

            // A dynamic-table size update carries no field.
            if (decoded.header.name.len == 0) continue;

            try headers.list.append(arena, .{
                .name = try arena.dupe(u8, decoded.header.name),
                .value = try arena.dupe(u8, decoded.header.value),
            });
        }
        return headers;
    }
};

/// DATA and HEADERS frames may carry padding; the pad length prefix and the
/// trailing pad bytes are not part of the payload.
fn stripPadding(frame: Frame) []const u8 {
    if (!frame.header.flags.has(FrameFlags.PADDED)) return frame.payload;
    if (frame.payload.len == 0) return frame.payload;
    const pad_len = frame.payload[0];
    const body = frame.payload[1..];
    if (pad_len > body.len) return body;
    return body[0 .. body.len - pad_len];
}

pub fn dataPayload(frame: Frame) []const u8 {
    return stripPadding(frame);
}

const testing = std.testing;

test "client preface is the exact byte sequence required by RFC 9113" {
    try testing.expectEqualStrings("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", client_preface);
    try testing.expectEqual(@as(usize, 24), client_preface.len);
}

test "stream ids follow the client-odd / server-even rule" {
    var r = std.Io.Reader.fixed("");
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    var client = Connection.init(testing.allocator, &r, &w, false);
    defer client.deinit();
    try testing.expectEqual(@as(u32, 1), client.openStream());
    try testing.expectEqual(@as(u32, 3), client.openStream());

    var server = Connection.init(testing.allocator, &r, &w, true);
    defer server.deinit();
    try testing.expectEqual(@as(u32, 2), server.openStream());
}

test "padding is stripped from a padded frame" {
    // pad length 2, body "hi", then 2 pad bytes.
    const payload = [_]u8{ 2, 'h', 'i', 0, 0 };
    const frame: Frame = .{
        .header = .{
            .length = payload.len,
            .frame_type = .DATA,
            .flags = FrameFlags.init(FrameFlags.PADDED),
            .reserved = false,
            .stream_id = 1,
        },
        .payload = &payload,
    };
    try testing.expectEqualStrings("hi", dataPayload(frame));
}

test "unpadded frames are returned untouched" {
    const payload = [_]u8{ 'h', 'i' };
    const frame: Frame = .{
        .header = .{
            .length = payload.len,
            .frame_type = .DATA,
            .flags = FrameFlags.init(0),
            .reserved = false,
            .stream_id = 1,
        },
        .payload = &payload,
    };
    try testing.expectEqualStrings("hi", dataPayload(frame));
}
