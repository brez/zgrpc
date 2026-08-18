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
//!
//! ## Shape
//!
//! One task owns the read side and does nothing else: `readLoop` pulls frames
//! off the socket forever, decodes header blocks, and files everything into
//! per-stream mailboxes. Callers never read from the socket. They wait on their
//! own stream, and any number of them can be doing so at once.
//!
//! This is the difference between noticing something and being told. When the
//! writer drove the reader, a RST_STREAM sat unread until the writer happened
//! to block for flow-control credit, so cancellation was only as prompt as the
//! peer's window was small. A dedicated reader sees it on arrival.
//!
//! ## Locking
//!
//! Three mutexes, held one at a time — never nested, in any order:
//!
//! * `mutex` guards connection and stream state: the stream table, both flow
//!   -control windows, every stream mailbox.
//! * `write_mutex` guards the writer and the HPACK encode table. One frame, or
//!   one HEADERS block with its CONTINUATIONs, is written under a single hold;
//!   interleaving either would corrupt the stream.
//! * `open_mutex` serialises opening a stream, so ids reach the peer in
//!   ascending order (RFC 9113 5.1.1) and a stream is always in the table
//!   before its HEADERS is on the wire.
//!
//! The no-nesting rule is what keeps a slow socket from stalling frame
//! processing: a writer blocked in `flush` holds no state lock, so the reader
//! keeps draining and keeps granting it credit. Nesting the other way — state
//! held across a flush — deadlocks against a peer that is itself blocked
//! writing to us.

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

/// Fixed by RFC 9113 4.1.
const frame_header_len = 9;

/// The window every HTTP/2 endpoint starts with, fixed by RFC 9113 6.9.2. It
/// is what the peer's windows are assumed to be until its SETTINGS says
/// otherwise, and what our own send windows start at.
pub const protocol_initial_window: i64 = 65_535;

/// What we advertise for streams the peer opens, and raise the connection to.
///
/// The 65_535 default is a latency bug at any real bandwidth: a sender may
/// have only that much in flight before it must stop and wait for credit, so
/// once bandwidth times round-trip exceeds it, every message larger than the
/// window costs an extra round trip. In-VPC at 10Gbps the bandwidth-delay
/// product is on the order of half a megabyte, which the default misses by an
/// order of magnitude. Measured on loopback, raising these cut the p50 for a
/// 128KB payload roughly in half.
///
/// The cost is memory: the connection window bounds what a peer can make us
/// buffer before we have consumed any of it, so it is also the per-connection
/// worst case. `Server.Options.max_concurrent_connections` multiplies it.
pub const default_stream_window: i64 = 256 * 1024;
pub const default_connection_window: i64 = 1024 * 1024;

/// Credit is handed back once this much has been consumed. Half the window
/// keeps WINDOW_UPDATE traffic to roughly one frame per half-window while
/// still refilling the sender long before it runs dry.
fn ackThreshold(window: i64) u32 {
    return @intCast(@divTrunc(window, 2));
}

/// How many concurrent streams we let a peer open. Advertised in SETTINGS, and
/// enforced: a peer that ignores it gets REFUSED_STREAM rather than unbounded
/// state on our side.
pub const default_max_concurrent_streams: u32 = 128;

/// SETTINGS parameter identifiers (RFC 9113 6.5.2).
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
pub const error_code_stream_closed: u32 = 0x5;
pub const error_code_frame_size_error: u32 = 0x6;
pub const error_code_refused_stream: u32 = 0x7;
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
    /// END_STREAM was set on the frame carrying this block, i.e. these are
    /// trailers (or a Trailers-Only response).
    end_stream: bool = false,

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

/// What a caller gets from `Stream.next`.
pub const Event = union(enum) {
    /// A decoded header block. The caller owns it and must `deinit` it.
    headers: Headers,
    /// This many bytes were copied into the buffer passed to `next`.
    data: usize,
    /// END_STREAM: the peer will send nothing further. Repeatable — asking
    /// again returns `end` again.
    end,
};

/// One HTTP/2 stream: a mailbox the reader files frames into and the owning
/// caller drains.
///
/// Every field is guarded by `conn.mutex`. Nothing here is touched without it.
pub const Stream = struct {
    conn: *Connection,
    id: u32,

    /// Signalled by the reader whenever something arrives for this stream.
    ready: std.Io.Condition = .init,

    /// Decoded header blocks awaiting collection, each tagged with how many
    /// stream bytes had arrived before it. That tag is what keeps a header
    /// block from overtaking DATA that preceded it — trailers must not be
    /// handed over before the body they follow.
    header_blocks: std.ArrayList(PendingHeaders) = .empty,

    /// Received DATA not yet handed to the caller.
    recv: std.ArrayList(u8) = .empty,
    recv_pos: usize = 0,
    /// Total DATA bytes received, and total handed over. The difference is
    /// what is sitting in `recv`.
    recv_total: usize = 0,
    taken_total: usize = 0,
    /// Bytes taken by the caller but not yet returned to the peer as
    /// flow-control credit.
    unacked: u32 = 0,

    /// END_STREAM received.
    ended: bool = false,
    /// RST_STREAM received. The point of the dedicated reader: this becomes
    /// true the moment the frame lands, not when a writer next blocks.
    reset: bool = false,

    /// Flow-control credit for DATA we send on this stream.
    send_window: i64,

    const PendingHeaders = struct {
        headers: Headers,
        /// `recv_total` at the moment the block arrived.
        after_bytes: usize,
    };

    fn destroy(self: *Stream) void {
        const allocator = self.conn.allocator;
        for (self.header_blocks.items) |*pending| pending.headers.deinit();
        self.header_blocks.deinit(allocator);
        self.recv.deinit(allocator);
        allocator.destroy(self);
    }

    /// Unread bytes sitting in the mailbox. Caller must hold `conn.mutex`.
    fn buffered(self: *const Stream) []const u8 {
        return self.recv.items[self.recv_pos..];
    }

    /// Waits for and returns the next thing to happen on this stream.
    ///
    /// DATA is copied into `data_buf`, which is why the caller supplies it:
    /// handing out a pointer into the mailbox would mean either holding the
    /// connection lock across the caller's use of it, or refusing to let the
    /// reader make progress. Passing the destination in costs one copy and
    /// avoids both.
    ///
    /// Returns `error.StreamReset` once the peer has cancelled.
    pub fn next(self: *Stream, data_buf: []u8) !Event {
        const conn = self.conn;
        const io = conn.io;

        var ack_stream: u32 = 0;
        var ack_conn: u32 = 0;

        const event = blk: {
            try conn.mutex.lock(io);
            defer conn.mutex.unlock(io);

            while (true) {
                // A reset stream has nothing left worth reporting; whatever is
                // in the mailbox is for a call the peer has already abandoned.
                if (self.reset) return error.StreamReset;

                // Strict arrival order between the two mailboxes: a header
                // block is only ready once the DATA that preceded it has been
                // handed over.
                if (self.header_blocks.items.len > 0 and
                    self.header_blocks.items[0].after_bytes <= self.taken_total)
                {
                    const pending = self.header_blocks.orderedRemove(0);
                    break :blk Event{ .headers = pending.headers };
                }

                const available = self.buffered();
                if (available.len > 0 and data_buf.len > 0) {
                    const n = @min(available.len, data_buf.len);
                    @memcpy(data_buf[0..n], available[0..n]);
                    self.recv_pos += n;
                    self.taken_total += n;
                    // Fully drained is the common case, and resetting the
                    // cursor costs nothing. Anything left is moved down later,
                    // by the append that actually needs the room — compacting
                    // on every take makes a message spanning many frames
                    // quadratic in its own size.
                    if (self.recv_pos == self.recv.items.len) {
                        self.recv.clearRetainingCapacity();
                        self.recv_pos = 0;
                    }

                    // Credit is returned on consumption, not on arrival.
                    // Returning it on arrival is what lets a peer stream
                    // without bound into a caller that is not keeping up.
                    self.unacked += @intCast(n);
                    conn.recv_unacked += @intCast(n);
                    conn.recv_outstanding -= n;
                    if (self.unacked >= ackThreshold(default_stream_window)) {
                        ack_stream = self.unacked;
                        self.unacked = 0;
                    }
                    if (conn.recv_unacked >= ackThreshold(default_connection_window)) {
                        ack_conn = conn.recv_unacked;
                        conn.recv_unacked = 0;
                    }
                    break :blk Event{ .data = n };
                }

                if (self.ended) break :blk Event.end;
                if (conn.closed) return conn.closeReason();

                try self.ready.wait(io, &conn.mutex);
            }
        };

        // Outside the state lock, per the no-nesting rule.
        if (ack_conn > 0) try conn.sendWindowUpdate(0, ack_conn);
        if (ack_stream > 0) try conn.sendWindowUpdate(self.id, ack_stream);
        if (ack_conn > 0 or ack_stream > 0) try conn.flush();

        return event;
    }

    /// Drops bytes already handed over, making room without growing. Caller
    /// must hold `conn.mutex`.
    fn compact(self: *Stream) void {
        if (self.recv_pos == 0) return;
        const remaining = self.recv.items.len - self.recv_pos;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.recv.items[0..remaining], self.recv.items[self.recv_pos..]);
        }
        self.recv.shrinkRetainingCapacity(remaining);
        self.recv_pos = 0;
    }

    /// True once the peer has reset this stream. Cheap enough to check before
    /// producing each message of a long response.
    ///
    /// (Stream-level; see `Connection.isUsable` for the connection.)
    pub fn isReset(self: *Stream) bool {
        const io = self.conn.io;
        self.conn.mutex.lockUncancelable(io);
        defer self.conn.mutex.unlock(io);
        return self.reset;
    }
};

pub const Connection = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    /// Read by `readLoop` and nothing else.
    reader: *std.Io.Reader,
    /// Guarded by `write_mutex`.
    writer: *std.Io.Writer,
    is_server: bool,

    /// Guards connection and stream state. See the locking note at the top.
    mutex: std.Io.Mutex = .init,
    /// Guards `writer`, `encode_table` and `header_buf`.
    write_mutex: std.Io.Mutex = .init,
    /// Serialises stream creation so ids go out in ascending order.
    open_mutex: std.Io.Mutex = .init,

    /// Signalled when send credit appears, or when a stream or the connection
    /// dies while someone is waiting for credit.
    flow: std.Io.Condition = .init,
    /// Signalled when a peer-initiated stream is queued for `accept`.
    accept_ready: std.Io.Condition = .init,
    /// Signalled when a locally-initiated stream closes, freeing a slot under
    /// the peer's SETTINGS_MAX_CONCURRENT_STREAMS.
    stream_slots: std.Io.Condition = .init,

    /// Separate HPACK contexts per direction: each endpoint keeps its own
    /// encoder table, and they must not be shared. `encode_table` belongs to
    /// `write_mutex`; `decode_table` belongs to the reader task, which is the
    /// only thing that ever decodes.
    encode_table: Hpack.DynamicTable,
    decode_table: Hpack.DynamicTable,

    /// Odd for clients, even for servers (RFC 9113 5.1.1). Guarded by
    /// `open_mutex`.
    next_stream_id: u32,

    peer_max_frame_size: u32 = default_max_frame_size,
    peer_initial_window: i64 = protocol_initial_window,
    peer_max_concurrent_streams: ?u32 = null,

    /// Connection-level flow-control window for data we send. Starts at the
    /// protocol default and grows only when the peer says so.
    send_window: i64 = protocol_initial_window,
    /// Bytes taken by callers but not yet acknowledged at connection level.
    recv_unacked: u32 = 0,
    /// Bytes received across all streams and not yet consumed. Bounded by the
    /// connection window, which is the only thing that bounds how much a peer
    /// can make one connection buffer.
    recv_outstanding: usize = 0,

    streams: std.AutoHashMapUnmanaged(u32, *Stream) = .empty,
    /// Peer-initiated streams waiting for `accept`.
    incoming: std.ArrayList(*Stream) = .empty,
    /// Streams we opened that are still live, counted against the peer's
    /// SETTINGS_MAX_CONCURRENT_STREAMS.
    local_streams: u32 = 0,
    /// Highest peer-initiated stream id seen, for the GOAWAY we send.
    last_peer_stream_id: u32 = 0,

    goaway_received: ?GoAway = null,
    /// Set when the reader stops, for any reason. Everything waiting is woken.
    closed: bool = false,
    /// Why the reader stopped, if it was not an orderly end.
    read_error: ?anyerror = null,

    /// Scratch for encoding header blocks, so a steady stream of calls does
    /// not allocate per request. Guarded by `write_mutex`.
    header_buf: std.ArrayList(u8) = .empty,
    /// Scratch for frame payloads. Reader task only.
    payload_buf: std.ArrayList(u8) = .empty,

    /// Streams with something new, not yet woken. Reader task only.
    ///
    /// Wakeups are deferred until the reader is about to block on the socket,
    /// because waking a caller and then immediately handing it more is a
    /// context switch spent for nothing. A unary response is three frames —
    /// HEADERS, DATA, trailers — written with one flush and so read in one
    /// go; woken per frame that is three wakeups where one will do.
    pending_wakes: std.ArrayList(u32) = .empty,
    wake_accept: bool = false,
    wake_flow: bool = false,

    reader_task: ?std.Io.Future(void) = null,

    /// `allocator` must be thread-safe: the reader task and every caller
    /// allocate from it concurrently.
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        reader: *std.Io.Reader,
        writer: *std.Io.Writer,
        is_server: bool,
    ) Connection {
        return .{
            .allocator = allocator,
            .io = io,
            .reader = reader,
            .writer = writer,
            .is_server = is_server,
            .encode_table = Hpack.DynamicTable.init(allocator, 4096),
            .decode_table = Hpack.DynamicTable.init(allocator, 4096),
            .next_stream_id = if (is_server) 2 else 1,
        };
    }

    /// Starts the reader. The `Connection` must already be at its final
    /// address — the task holds a pointer to it.
    pub fn start(self: *Connection) !void {
        self.reader_task = try self.io.concurrent(readLoop, .{self});
    }

    /// Stops the reader and wakes everything waiting on this connection.
    ///
    /// Idempotent, and safe to call with calls still in flight: they see the
    /// connection closed and unwind.
    pub fn shutdown(self: *Connection) void {
        if (self.reader_task) |*task| {
            _ = task.cancel(self.io);
            self.reader_task = null;
        }
        self.markClosed(null);
    }

    pub fn deinit(self: *Connection) void {
        self.shutdown();

        var it = self.streams.valueIterator();
        while (it.next()) |stream| stream.*.destroy();
        self.streams.deinit(self.allocator);
        self.incoming.deinit(self.allocator);

        self.encode_table.deinit();
        self.decode_table.deinit();
        self.header_buf.deinit(self.allocator);
        self.payload_buf.deinit(self.allocator);
        self.pending_wakes.deinit(self.allocator);
    }

    /// Whether this connection can take a new stream.
    ///
    /// False once the reader has stopped or the peer has said GOAWAY. Either
    /// way the calls already running may still finish — GOAWAY means "open no
    /// more streams", not "abandon the ones in progress" — so this gates new
    /// work rather than existing work.
    pub fn isUsable(self: *Connection) bool {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return !self.closed and self.goaway_received == null;
    }

    /// The error a caller should see for a connection that is no longer
    /// running. Caller must hold `mutex`.
    fn closeReason(self: *const Connection) anyerror {
        if (self.read_error) |err| return err;
        return error.ConnectionClosed;
    }

    fn markClosed(self: *Connection, err: ?anyerror) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (!self.closed) {
            self.closed = true;
            self.read_error = err;
        }
        // Everyone waiting on anything has to be told, or a caller blocked on
        // a stream that will never see another frame waits forever.
        var it = self.streams.valueIterator();
        while (it.next()) |stream| stream.*.ready.broadcast(io);
        self.flow.broadcast(io);
        self.accept_ready.broadcast(io);
        self.stream_slots.broadcast(io);
    }

    // -- streams -----------------------------------------------------------

    /// Opens a locally-initiated stream and writes its HEADERS.
    ///
    /// Registration happens before the frame goes out, so a response cannot
    /// arrive for a stream the reader has never heard of, and `open_mutex`
    /// keeps concurrent opens in ascending id order.
    pub fn openStream(
        self: *Connection,
        headers: []const Header,
        end_stream: bool,
    ) !*Stream {
        const io = self.io;
        try self.open_mutex.lock(io);
        defer self.open_mutex.unlock(io);

        const stream = try self.allocator.create(Stream);
        errdefer self.allocator.destroy(stream);

        {
            try self.mutex.lock(io);
            defer self.mutex.unlock(io);

            // Wait rather than fail when the peer's stream limit is reached.
            // Exceeding it is a protocol violation the peer answers with
            // REFUSED_STREAM, and a caller that issued one call too many would
            // rather be queued for a moment than told no.
            const limit = self.peer_max_concurrent_streams orelse default_max_concurrent_streams;
            while (self.local_streams >= limit) {
                if (self.closed) return self.closeReason();
                if (self.goaway_received != null) return error.ConnectionClosed;
                try self.stream_slots.wait(io, &self.mutex);
            }

            if (self.closed) return self.closeReason();
            if (self.goaway_received != null) return error.ConnectionClosed;

            const id = self.next_stream_id;
            self.next_stream_id += 2;
            stream.* = .{ .conn = self, .id = id, .send_window = self.peer_initial_window };
            try self.streams.put(self.allocator, id, stream);
            self.local_streams += 1;
        }
        errdefer self.forget(stream.id);

        // Deliberately not flushed: the caller writes the request body next,
        // and HEADERS plus DATA in one segment is one wakeup at the peer
        // instead of two. Callers flush when the request is complete.
        try self.writeHeaders(stream.id, headers, end_stream);
        return stream;
    }

    /// Waits for the next stream the peer opens. Null once the connection has
    /// finished — the natural end of a server's accept loop.
    pub fn acceptStream(self: *Connection) !?*Stream {
        const io = self.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        while (true) {
            if (self.incoming.items.len > 0) return self.incoming.orderedRemove(0);
            if (self.closed) {
                if (self.read_error) |err| return err;
                return null;
            }
            if (self.goaway_received != null) return null;
            try self.accept_ready.wait(io, &self.mutex);
        }
    }

    /// Finishes with a stream. A stream abandoned before the peer ended it is
    /// cancelled, which is what stops the peer producing into it.
    pub fn closeStream(self: *Connection, stream: *Stream) void {
        const io = self.io;
        const id = stream.id;

        const needs_reset = blk: {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            break :blk !stream.ended and !stream.reset and !self.closed;
        };

        if (needs_reset) {
            self.sendRstStream(id, error_code_cancel) catch {};
            self.flush() catch {};
        }
        self.forget(id);
    }

    fn forget(self: *Connection, id: u32) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        const entry = self.streams.fetchRemove(id) orelse return;

        // Bytes buffered for a stream nobody will read still cost connection
        // window. Dropping them without returning the credit shrinks the
        // window for good, and a long-lived connection would throttle itself
        // to a halt one abandoned call at a time.
        const unread = entry.value.buffered().len;
        self.recv_outstanding -= unread;
        self.recv_unacked += @intCast(unread);
        entry.value.destroy();

        const locally_opened = if (self.is_server) id % 2 == 0 else id % 2 == 1;
        if (locally_opened) {
            self.local_streams -= 1;
            self.stream_slots.signal(io);
        }
    }

    // -- handshake ---------------------------------------------------------

    /// Client side: preface bytes followed by our SETTINGS and connection
    /// window.
    pub fn sendClientPreface(self: *Connection) !void {
        {
            const io = self.io;
            try self.write_mutex.lock(io);
            defer self.write_mutex.unlock(io);

            try self.writer.writeAll(client_preface);
            try self.writeSettingsLocked();
        }
        try self.sendWindowUpdate(0, @intCast(default_connection_window - protocol_initial_window));
    }

    /// Server side: validate the client preface before anything else, so a
    /// plain HTTP/1 or garbage connection fails immediately and clearly.
    ///
    /// Called before `start`, on the connection's own task.
    pub fn readClientPreface(self: *Connection) !void {
        var buf: [client_preface.len]u8 = undefined;
        try self.reader.readSliceAll(&buf);
        if (!std.mem.eql(u8, &buf, client_preface)) return error.InvalidPreface;
    }

    /// Our SETTINGS, followed by the WINDOW_UPDATE that raises the connection
    /// window.
    ///
    /// The connection window cannot be set in SETTINGS — RFC 9113 6.9.2 fixes
    /// its initial value at 65_535 and only a WINDOW_UPDATE on stream 0 moves
    /// it — so raising it is a separate frame sent at the same moment.
    pub fn sendSettings(self: *Connection) !void {
        {
            const io = self.io;
            try self.write_mutex.lock(io);
            defer self.write_mutex.unlock(io);
            try self.writeSettingsLocked();
        }
        try self.sendWindowUpdate(0, @intCast(default_connection_window - protocol_initial_window));
    }

    fn writeSettingsLocked(self: *Connection) !void {
        // Advertise no server push: gRPC never uses it, and refusing up front
        // avoids having to handle PUSH_PROMISE at all.
        const params = [_]struct { id: u16, value: u32 }{
            .{ .id = setting_enable_push, .value = 0 },
            .{ .id = setting_max_frame_size, .value = default_max_frame_size },
            .{ .id = setting_initial_window_size, .value = @intCast(default_stream_window) },
            .{ .id = setting_max_concurrent_streams, .value = default_max_concurrent_streams },
        };

        var payload: [params.len * 6]u8 = undefined;
        for (params, 0..) |p, i| {
            std.mem.writeInt(u16, payload[i * 6 ..][0..2], p.id, .big);
            std.mem.writeInt(u32, payload[i * 6 + 2 ..][0..4], p.value, .big);
        }
        try self.writeFrameLocked(.SETTINGS, 0, 0, &payload);
    }

    // -- frame writing -----------------------------------------------------

    /// Caller must hold `write_mutex`.
    fn writeFrameLocked(
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

    fn writeFrame(
        self: *Connection,
        frame_type: FrameType,
        flags: u8,
        stream_id: u32,
        payload: []const u8,
    ) !void {
        const io = self.io;
        try self.write_mutex.lock(io);
        defer self.write_mutex.unlock(io);
        try self.writeFrameLocked(frame_type, flags, stream_id, payload);
    }

    /// Writes a HEADERS frame, splitting into CONTINUATION frames when the
    /// encoded block exceeds the peer's max frame size.
    ///
    /// This is used for request headers, response headers *and* trailers —
    /// trailers are simply a header block with END_STREAM and no
    /// pseudo-headers. The whole block goes out under one hold of
    /// `write_mutex`: a HEADERS/CONTINUATION sequence interrupted by any other
    /// frame is a connection error.
    pub fn writeHeaders(
        self: *Connection,
        stream_id: u32,
        headers: []const Header,
        end_stream: bool,
    ) !void {
        const io = self.io;
        try self.write_mutex.lock(io);
        defer self.write_mutex.unlock(io);

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
        try self.writeFrameLocked(.HEADERS, flags, stream_id, block[0..first_len]);

        var offset = first_len;
        while (offset < block.len) {
            const chunk_len = @min(block.len - offset, max);
            const last = offset + chunk_len == block.len;
            try self.writeFrameLocked(
                .CONTINUATION,
                if (last) FrameFlags.END_HEADERS else 0,
                stream_id,
                block[offset..][0..chunk_len],
            );
            offset += chunk_len;
        }
    }

    /// Writes DATA, respecting the peer's max frame size and both
    /// flow-control windows.
    ///
    /// Credit is reserved under `mutex` and the frame written under
    /// `write_mutex`, never both at once. Two streams writing at the same time
    /// therefore interleave whole frames, which is exactly what HTTP/2
    /// multiplexing is.
    pub fn writeData(
        self: *Connection,
        stream: *Stream,
        data: []const u8,
        end_stream: bool,
    ) !void {
        if (data.len == 0) {
            try self.writeFrame(
                .DATA,
                if (end_stream) FrameFlags.END_STREAM else 0,
                stream.id,
                "",
            );
            return;
        }

        var offset: usize = 0;
        while (offset < data.len) {
            const chunk_len = try self.reserveCredit(stream, data.len - offset);
            const last = offset + chunk_len == data.len;
            try self.writeFrame(
                .DATA,
                if (last and end_stream) FrameFlags.END_STREAM else 0,
                stream.id,
                data[offset..][0..chunk_len],
            );
            offset += chunk_len;
        }
    }

    /// Blocks until this stream has send credit, then takes some.
    ///
    /// Anything already buffered has to reach the peer before waiting, or it
    /// will never send the WINDOW_UPDATE being waited for.
    fn reserveCredit(self: *Connection, stream: *Stream, want: usize) !usize {
        const io = self.io;
        var flushed = false;

        while (true) {
            {
                try self.mutex.lock(io);
                defer self.mutex.unlock(io);

                if (stream.reset) return error.StreamReset;
                if (self.closed) return self.closeReason();
                if (self.goaway_received != null) return error.ConnectionClosed;

                const available = @min(self.send_window, stream.send_window);
                if (available > 0) {
                    const limit: usize = @intCast(available);
                    const n = @min(@min(want, limit), self.peer_max_frame_size);
                    self.send_window -= @intCast(n);
                    stream.send_window -= @intCast(n);
                    return n;
                }

                if (flushed) {
                    try self.flow.wait(io, &self.mutex);
                    continue;
                }
            }

            try self.flush();
            flushed = true;
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
        const io = self.io;
        try self.write_mutex.lock(io);
        defer self.write_mutex.unlock(io);
        try self.writer.flush();
    }

    // -- frame reading -----------------------------------------------------

    /// Marks a stream as having something new. Caller must hold `mutex`.
    ///
    /// Ids rather than pointers: the stream's owner may close it before the
    /// wakeup is delivered, and the lookup at delivery time is what makes that
    /// safe.
    fn noteWake(self: *Connection, stream_id: u32) !void {
        for (self.pending_wakes.items) |id| {
            if (id == stream_id) return;
        }
        try self.pending_wakes.append(self.allocator, stream_id);
    }

    /// Delivers every deferred wakeup. Called only when the reader is about to
    /// block, so nothing waits longer than one frame's processing.
    fn flushWakes(self: *Connection) void {
        if (self.pending_wakes.items.len == 0 and !self.wake_accept and !self.wake_flow) return;

        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        for (self.pending_wakes.items) |id| {
            if (self.streams.get(id)) |stream| stream.ready.broadcast(io);
        }
        self.pending_wakes.clearRetainingCapacity();

        if (self.wake_accept) {
            self.accept_ready.broadcast(io);
            self.wake_accept = false;
        }
        if (self.wake_flow) {
            self.flow.broadcast(io);
            self.wake_flow = false;
        }
    }

    /// The reader task. Owns `reader`, `decode_table` and `payload_buf`, and
    /// runs until the connection ends or it is cancelled.
    fn readLoop(self: *Connection) void {
        while (true) {
            self.readFrame() catch |err| switch (err) {
                // The peer hanging up, or us shutting down. Neither is a
                // failure worth reporting to callers as one.
                error.EndOfStream, error.Canceled, error.ConnectionResetByPeer => {
                    self.markClosed(null);
                    return;
                },
                else => {
                    self.markClosed(err);
                    return;
                },
            };
            if (self.sawGoAway()) {
                self.markClosed(null);
                return;
            }
        }
    }

    fn sawGoAway(self: *Connection) bool {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.goaway_received != null;
    }

    /// Reads and dispatches exactly one frame.
    ///
    /// `anyerror` so `readLoop` can name the disconnect cases explicitly; the
    /// inferred set varies with the `Io` implementation in use.
    fn readFrame(self: *Connection) anyerror!void {
        // A frame header is exactly 9 bytes. Anything less buffered means the
        // next read blocks, which is the moment deferred wakeups are owed.
        if (self.reader.bufferedLen() < frame_header_len) self.flushWakes();
        const header = try FrameHeader.read(self.reader);

        if (header.length > 0) {
            if (self.reader.bufferedLen() < header.length) self.flushWakes();
            try self.payload_buf.resize(self.allocator, header.length);
            try self.reader.readSliceAll(self.payload_buf.items);
        } else {
            self.payload_buf.clearRetainingCapacity();
        }
        const frame: Frame = .{ .header = header, .payload = self.payload_buf.items };

        switch (header.frame_type) {
            .SETTINGS => {
                if (header.flags.has(FrameFlags.ACK)) return;
                try self.applySettings(frame.payload);
                try self.writeFrame(.SETTINGS, FrameFlags.ACK, 0, "");
                try self.flush();
            },
            .PING => {
                if (header.flags.has(FrameFlags.ACK)) return;
                try self.writeFrame(.PING, FrameFlags.ACK, 0, frame.payload);
                try self.flush();
            },
            .WINDOW_UPDATE => try self.applyWindowUpdate(frame),
            .GOAWAY => try self.applyGoAway(frame),
            .RST_STREAM => try self.applyRstStream(frame),
            .HEADERS => try self.applyHeaders(frame),
            .DATA => try self.applyData(frame),
            // PRIORITY and anything unknown: ignored, per RFC 9113 4.1.
            else => {},
        }
    }

    fn applySettings(self: *Connection, payload: []const u8) !void {
        if (payload.len % 6 != 0) return error.InvalidFrameSize;

        const io = self.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

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
                    // (RFC 9113 6.9.2).
                    const delta = @as(i64, value) - self.peer_initial_window;
                    self.peer_initial_window = value;
                    var it = self.streams.valueIterator();
                    while (it.next()) |stream| stream.*.send_window += delta;
                    if (delta > 0) self.wake_flow = true;
                },
                setting_max_concurrent_streams => self.peer_max_concurrent_streams = value,
                // The encode table lives under `write_mutex`, which must not
                // be taken here. Shrinking it is optional (RFC 9113 6.5.2
                // makes the *encoder* free to use less than advertised), so
                // the safe reading is to keep using the smaller default.
                setting_header_table_size => {},
                setting_enable_push, setting_max_header_list_size => {},
                else => {},
            }
        }
    }

    fn applyWindowUpdate(self: *Connection, frame: Frame) !void {
        if (frame.payload.len != 4) return error.InvalidFrameSize;
        const increment = std.mem.readInt(u32, frame.payload[0..4], .big) & 0x7fff_ffff;
        if (increment == 0) return error.ProtocolError;

        const io = self.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        if (frame.header.stream_id == 0) {
            self.send_window += increment;
        } else if (self.streams.get(frame.header.stream_id)) |stream| {
            stream.send_window += increment;
        }
        self.wake_flow = true;
    }

    fn applyGoAway(self: *Connection, frame: Frame) !void {
        if (frame.payload.len < 8) return error.InvalidFrameSize;

        const io = self.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        self.goaway_received = .{
            .last_stream_id = std.mem.readInt(u32, frame.payload[0..4], .big) & 0x7fff_ffff,
            .error_code = std.mem.readInt(u32, frame.payload[4..8], .big),
        };
        self.wake_flow = true;
        self.wake_accept = true;
    }

    fn applyRstStream(self: *Connection, frame: Frame) !void {
        const io = self.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        if (self.streams.get(frame.header.stream_id)) |stream| {
            stream.reset = true;
            try self.noteWake(stream.id);
        }
        // Woken regardless: a writer parked for credit on this stream has to
        // be told, which is the whole reason cancellation is prompt.
        self.wake_flow = true;
    }

    fn applyHeaders(self: *Connection, frame: Frame) !void {
        // Always decoded, even for a stream we will discard. HPACK's dynamic
        // table is connection-wide state built by every header block in order,
        // so one skipped block corrupts every later block on the connection.
        var headers = try self.readHeaderBlock(frame);
        var owned = true;
        defer if (owned) headers.deinit();

        headers.end_stream = frame.header.flags.has(FrameFlags.END_STREAM);
        const id = frame.header.stream_id;

        const io = self.io;
        var refuse = false;
        {
            try self.mutex.lock(io);
            defer self.mutex.unlock(io);

            const stream = self.streams.get(id) orelse blk: {
                // Not a stream we know. Peer-initiated ids open a new one;
                // anything else is for a call that has already been cleaned
                // up, and only the HPACK side effect above mattered.
                const peer_initiated = if (self.is_server) id % 2 == 1 else id % 2 == 0;
                if (!peer_initiated or id <= self.last_peer_stream_id) return;

                if (self.streams.count() >= default_max_concurrent_streams) {
                    refuse = true;
                    break :blk null;
                }

                const created = try self.allocator.create(Stream);
                created.* = .{ .conn = self, .id = id, .send_window = self.peer_initial_window };
                try self.streams.put(self.allocator, id, created);
                self.last_peer_stream_id = id;
                try self.incoming.append(self.allocator, created);
                self.wake_accept = true;
                break :blk created;
            } orelse null;

            if (stream) |s| {
                try s.header_blocks.append(self.allocator, .{
                    .headers = headers,
                    .after_bytes = s.recv_total,
                });
                owned = false;
                if (headers.end_stream) s.ended = true;
                try self.noteWake(s.id);
            }
        }

        if (refuse) {
            self.sendRstStream(id, error_code_refused_stream) catch {};
            self.flush() catch {};
        }
    }

    fn applyData(self: *Connection, frame: Frame) !void {
        const payload = stripPadding(frame);
        const id = frame.header.stream_id;
        const end_stream = frame.header.flags.has(FrameFlags.END_STREAM);

        const io = self.io;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const stream = self.streams.get(id) orelse {
            // Data for a stream we have finished with still counts against the
            // connection window, so the credit has to be returned or the
            // connection slowly throttles itself to a halt. Nothing is
            // buffered, so it never counts towards `recv_outstanding`.
            self.recv_unacked += frame.header.length;
            if (self.recv_unacked >= ackThreshold(default_connection_window)) {
                const increment = self.recv_unacked;
                self.recv_unacked = 0;
                self.mutex.unlock(io);
                defer self.mutex.lockUncancelable(io);
                try self.sendWindowUpdate(0, increment);
                try self.flush();
            }
            return;
        };

        // The peer may not send more than the windows we advertised. Buffering
        // past them would let it choose how much memory we spend — the whole
        // point of tracking a receive window rather than trusting the sender.
        if (stream.buffered().len + stream.unacked + payload.len > default_stream_window) {
            return error.FlowControlError;
        }
        if (self.recv_outstanding + payload.len > default_connection_window) {
            return error.FlowControlError;
        }

        // Reclaim consumed space before growing, so a stream that is being
        // drained as fast as it fills reuses one buffer forever.
        if (stream.recv_pos > 0 and stream.recv.items.len + payload.len > stream.recv.capacity) {
            stream.compact();
        }
        try stream.recv.appendSlice(self.allocator, payload);
        stream.recv_total += payload.len;
        self.recv_outstanding += payload.len;
        if (end_stream) stream.ended = true;
        try self.noteWake(stream.id);
    }

    /// Reads a complete header block, following CONTINUATION frames, and
    /// decodes it. Reader task only.
    ///
    /// `first` must be the HEADERS frame already read from the wire.
    fn readHeaderBlock(self: *Connection, first: Frame) !Headers {
        // Copy the first fragment out: reading CONTINUATION frames reuses
        // `payload_buf` and would otherwise clobber it.
        var block: std.ArrayList(u8) = .empty;
        defer block.deinit(self.allocator);
        try block.appendSlice(self.allocator, stripPadding(first));

        var end_headers = first.header.flags.has(FrameFlags.END_HEADERS);
        while (!end_headers) {
            const header = try FrameHeader.read(self.reader);
            if (header.frame_type != .CONTINUATION) return error.ExpectedContinuation;
            if (header.stream_id != first.header.stream_id) return error.ProtocolError;

            try self.payload_buf.resize(self.allocator, header.length);
            try self.reader.readSliceAll(self.payload_buf.items);
            try block.appendSlice(self.allocator, self.payload_buf.items);
            end_headers = header.flags.has(FrameFlags.END_HEADERS);
        }

        return self.decodeHeaderBlock(block.items);
    }

    /// Decodes an HPACK block into owned headers.
    ///
    /// Upstream's `decodeHeaderFieldView` resets a thread-local scratch buffer
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

    var client = Connection.init(testing.allocator, testing.io, &r, &w, false);
    defer client.deinit();
    try testing.expectEqual(@as(u32, 1), client.next_stream_id);

    var server = Connection.init(testing.allocator, testing.io, &r, &w, true);
    defer server.deinit();
    try testing.expectEqual(@as(u32, 2), server.next_stream_id);
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
