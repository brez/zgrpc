//! gRPC server over h2c: unary and server-streaming methods.
//!
//! Concurrency, outermost first: `serve` gives every connection its own unit
//! of concurrency, and every connection gives every call its own. Frames are
//! read by the connection's reader task in `h2.zig` and filed into per-stream
//! mailboxes, so a handler that is slow, blocked, or streaming a million rows
//! holds up nothing but itself.
//!
//! One rule this structure exists to keep: a connection's header blocks are
//! decoded on exactly one task. The HPACK decoder in the upstream `http2.zig`
//! dependency hands back slices into a *thread-local* scratch buffer that it
//! resets on entry to every field, and its dynamic table is connection-wide
//! ordered state. Different threads are fine; two of a connection's blocks
//! interleaved on one thread are not. Decoding lives in the reader task alone,
//! which settles it.
const std = @import("std");
const h2 = @import("h2.zig");
const framing = @import("framing.zig");
const status = @import("status.zig");
const timeout = @import("timeout.zig");
const tcp = @import("tcp.zig");

const log = std.log.scoped(.zgrpc);

const Status = status.Status;
const Code = status.Code;

/// How much room to offer the transport for each read. One max-size HTTP/2
/// frame, so a full DATA frame is handed over in one go.
const recv_chunk = h2.default_max_frame_size;

/// Deadlines are measured on a clock that does not move when the system clock
/// is adjusted; see `client.deadline_clock`.
const deadline_clock: std.Io.Clock = .awake;

/// Everything a handler learns about the call in progress.
pub const CallContext = struct {
    /// Per-call arena. Anything the handler allocates from this — including
    /// the response it returns — is freed when the call completes, so handlers
    /// never need their own cleanup.
    allocator: std.mem.Allocator,
    /// The server's `Io`. Handlers that do their own I/O — query a store, call
    /// another service, wait on anything — need this rather than blocking the
    /// connection's task outside the `Io` implementation's knowledge.
    io: std.Io,
    /// Full method path, e.g. `/helloworld.Greeter/SayHello`.
    path: []const u8,
    /// When this call must be over, if the client sent `grpc-timeout`.
    ///
    /// The transport enforces this wherever it blocks on the client's behalf:
    /// reading the request, and `ServerStream.send`. What it cannot do is
    /// interrupt the handler's own work — there is no way to preempt a
    /// function — so a handler that does something slow should bound it with
    /// `deadlineTimeout` or check `expired`.
    ///
    /// Measured from when the request headers were read, not from when the
    /// client sent them: `grpc-timeout` is a duration, and the time it spent
    /// in flight is not knowable without synchronised clocks. The server's
    /// deadline is therefore always a little later than the client's, which is
    /// the safe direction to be wrong in.
    deadline: ?std.Io.Clock.Timestamp = null,
    request_headers: *const h2.Headers,

    /// Status returned to the client. A handler that wants to fail with a
    /// specific code sets this and returns an empty body.
    status: Status = Status.ok,

    /// A piece of request metadata the client sent, by name (any case), or
    /// null. If the client sent the name more than once this is the first.
    /// Pseudo-headers are not metadata and are never returned.
    ///
    /// The slice lives as long as the call.
    pub fn metadata(self: *const CallContext, name: []const u8) ?[]const u8 {
        if (name.len == 0 or name[0] == ':') return null;
        return self.request_headers.get(name);
    }

    pub fn fail(self: *CallContext, code: Code, message: []const u8) error{HandlerFailed} {
        self.status = .{ .code = code, .message = message };
        return error.HandlerFailed;
    }

    /// Time left, or null if the client set no deadline. Negative once it has
    /// passed.
    pub fn remaining(self: *const CallContext) ?std.Io.Duration {
        const at = self.deadline orelse return null;
        return at.durationFromNow(self.io).raw;
    }

    pub fn expired(self: *const CallContext) bool {
        const left = self.remaining() orelse return false;
        return left.nanoseconds <= 0;
    }

    /// The deadline in the form any `Io` call that accepts one will take, so a
    /// handler can hand its own work the same limit the call has.
    pub fn deadlineTimeout(self: *const CallContext) std.Io.Timeout {
        const at = self.deadline orelse return .none;
        return .{ .deadline = at };
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
    stream: *h2.Stream,
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
    /// Returns `error.StreamReset` once the client has cancelled, or
    /// `error.DeadlineExceeded` once the call's deadline has passed. Either
    /// unwinds the handler and stops it producing; handlers that loop over a
    /// large result set get both for free from `try stream.send(...)`.
    ///
    /// The check is against state the connection's reader task has already
    /// recorded, so it reflects a RST_STREAM that arrived at any point — not
    /// only one noticed because this write happened to block.
    pub fn send(self: *ServerStream, message: []const u8) !void {
        if (self.stream.isReset()) return error.StreamReset;
        // Checked per message so a handler looping over a large result set
        // stops at the deadline, rather than only when the transport next
        // happens to block.
        if (self.stream.isExpired()) return error.DeadlineExceeded;

        if (!self.headers_sent) {
            try self.conn.writeHeaders(self.stream.id, &.{
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

        try self.conn.writeData(self.stream, self.frame_buf.items, false);
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
    /// Ceiling on connections served at once.
    ///
    /// This bounds concurrency, not request rate: gRPC connections are
    /// long-lived and multiplex calls, so a handful of clients need a handful
    /// of slots however many RPCs they issue. It exists because each slot
    /// costs a unit of concurrency (a thread, under `std.Io.Threaded`) plus
    /// `2 * io_buffer_bytes`, and accepting without a bound lets a peer decide
    /// how much of both to spend.
    max_concurrent_connections: usize = 128,
    /// Drops a peer that stops answering, which is what frees the slot of a
    /// client that vanished. Null disables it, and a vanished client then holds
    /// its slot until the kernel gives up on the socket, which is hours.
    keepalive: ?h2.Keepalive = .{ .interval_ns = 30 * std.time.ns_per_s, .timeout_ns = 10 * std.time.ns_per_s },
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    methods: []const Method,
    options: Options,
    net_server: std.Io.net.Server,

    /// Connections in flight. Spawned tasks hold a pointer to the `Server`, so
    /// it must not move once `serve` has been called.
    connections: std.Io.Group = .init,
    /// Free connection slots; see `Options.max_concurrent_connections`.
    slots: std.Io.Semaphore,

    /// Connections being served, so `shutdown` can reach them. A connection
    /// lives on its own task's stack, which is why this holds pointers and why
    /// removal (under the same mutex) happens before that stack goes away.
    registry_mutex: std.Io.Mutex = .init,
    registry: std.ArrayList(*h2.Connection) = .empty,
    /// Connection tasks still running, for `shutdown` to wait on.
    live: std.atomic.Value(u32) = .init(0),

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
            .slots = .{ .permits = options.max_concurrent_connections },
        };
    }

    /// Cancels every connection still in flight and stops listening.
    ///
    /// Cancelling before closing the socket, rather than after, is what makes
    /// this safe to call while `serve` is running: a task blocked in `read`
    /// gets `error.Canceled` from that read and unwinds normally, instead of
    /// waking up later against a closed descriptor.
    pub fn deinit(self: *Server) void {
        self.connections.cancel(self.io);
        self.net_server.deinit(self.io);
        self.registry.deinit(self.allocator);
    }

    /// Stops serving gracefully: connections are told to open no more calls,
    /// and calls already running get up to `grace_ns` to finish. Returns true
    /// if every connection ended within that time.
    ///
    /// Stop the task running `serve` first; this does not stop accepting. What
    /// outlasts the grace period is left running, and `deinit` is what cuts it.
    /// Clients see GOAWAY, finish their calls, and stop using the connection,
    /// so a rolling restart costs them nothing.
    pub fn shutdown(self: *Server, grace_ns: u64) bool {
        const io = self.io;
        {
            self.registry_mutex.lockUncancelable(io);
            defer self.registry_mutex.unlock(io);
            // Under the mutex, because a connection leaves the registry before
            // its stack goes away and this must not touch one that has gone.
            for (self.registry.items) |conn| conn.beginDrain() catch {};
        }

        const deadline: std.Io.Clock.Timestamp = .fromNow(io, .{
            .raw = .fromNanoseconds(@intCast(grace_ns)),
            .clock = .awake,
        });
        while (true) {
            const remaining = self.live.load(.acquire);
            if (remaining == 0) return true;
            if (deadline.durationFromNow(io).raw.nanoseconds <= 0) return false;
            io.futexWaitTimeout(u32, &self.live.raw, remaining, .{ .deadline = deadline }) catch return false;
        }
    }

    /// Accepts connections until cancelled, serving each on its own unit of
    /// concurrency.
    ///
    /// Returns `error.Canceled` when the caller cancels it, which is the
    /// ordinary way to stop a server rather than a failure.
    pub fn serve(self: *Server) !void {
        // Being cancelled is how a graceful shutdown starts, so it must leave
        // the connections alone: `shutdown` drains them and `deinit` cuts
        // whatever is left. Any other failure is the server itself breaking.
        self.serveLoop() catch |err| {
            if (err != error.Canceled) self.connections.cancel(self.io);
            return err;
        };
    }

    fn serveLoop(self: *Server) !void {
        while (true) {
            // Taken before the accept, not after: waiting here leaves an
            // over-quota connection in the kernel's backlog, where the peer
            // sees a slow server. Accepting first and then blocking would let
            // it look connected while nothing was ever going to read it.
            try self.slots.wait(self.io);

            const stream = self.net_server.accept(self.io) catch |err| {
                self.slots.post(self.io);
                return err;
            };

            // `concurrent` rather than `async`: `async` is allowed to run the
            // task inline, which for a long-lived connection would mean never
            // returning to accept the next one.
            self.connections.concurrent(self.io, connectionTask, .{ self, stream }) catch {
                // No unit of concurrency to be had. Serving it inline is worse
                // than serving it in parallel but much better than dropping
                // it, and it applies backpressure to the accept loop for
                // exactly as long as the shortage lasts.
                connectionTask(self, stream);
            };
        }
    }

    /// One connection, start to finish. Errors are logged rather than
    /// returned: nothing is waiting on an individual connection's outcome, and
    /// one peer hanging up must not stop the server.
    fn connectionTask(self: *Server, stream: std.Io.net.Stream) void {
        _ = self.live.fetchAdd(1, .acq_rel);
        // Last, after the socket is closed and the slot returned, so a
        // `shutdown` that sees zero is looking at a finished connection.
        defer {
            _ = self.live.fetchSub(1, .acq_rel);
            self.io.futexWake(u32, &self.live.raw, std.math.maxInt(u32));
        }
        defer self.slots.post(self.io);
        defer stream.close(self.io);

        self.serveConnection(stream) catch |err| switch (err) {
            // The ordinary ends of a connection: the peer hung up, said
            // GOAWAY, or the server is shutting down.
            error.EndOfStream,
            error.ConnectionClosed,
            error.ConnectionResetByPeer,
            error.Canceled,
            => {},
            error.KeepaliveTimeout => log.info("dropped a peer that stopped answering", .{}),
            else => log.warn("connection failed: {t}", .{err}),
        };
    }

    /// The address actually bound, with the ephemeral port resolved — the
    /// caller needs this when listening on port 0.
    pub fn boundAddress(self: *const Server) std.Io.net.IpAddress {
        return self.net_server.socket.address;
    }

    /// Accepts one connection and serves calls on it until the peer closes it
    /// or sends GOAWAY, on the calling task.
    ///
    /// `serve` is what a real server wants; this is for tests and for callers
    /// that want to drive the accept loop themselves.
    pub fn acceptAndServe(self: *Server) !void {
        const stream = try self.net_server.accept(self.io);
        defer stream.close(self.io);
        try self.serveConnection(stream);
    }

    pub fn serveConnection(self: *Server, socket: std.Io.net.Stream) !void {
        try tcp.setNoDelay(socket);

        const read_buf = try self.allocator.alloc(u8, self.options.io_buffer_bytes);
        defer self.allocator.free(read_buf);
        const write_buf = try self.allocator.alloc(u8, self.options.io_buffer_bytes);
        defer self.allocator.free(write_buf);

        var reader = socket.reader(self.io, read_buf);
        var writer = socket.writer(self.io, write_buf);

        var conn = h2.Connection.init(
            self.allocator,
            self.io,
            &reader.interface,
            &writer.interface,
            true,
        );
        defer conn.deinit();

        conn.keepalive = self.options.keepalive;

        // The preface is read on this task, before the reader starts: it is
        // not a frame, and letting the frame loop try to parse it would turn a
        // clear "this is not HTTP/2" into a confusing framing error.
        try conn.readClientPreface();
        try conn.sendSettings();
        try conn.flush();
        try conn.start();

        // Registered only once started and removed before `conn` is torn down,
        // so `shutdown` never holds a pointer to a connection that is going.
        try self.register(&conn);
        defer self.unregister(&conn);

        // Declared after `conn` so it is cancelled first: a call task holds a
        // pointer to the connection.
        var calls: std.Io.Group = .init;
        defer calls.cancel(self.io);

        while (try conn.acceptStream()) |stream| {
            calls.concurrent(self.io, callTask, .{ self, &conn, stream }) catch {
                // Out of concurrency. Serving inline stops this connection
                // accepting further streams until the call finishes, which is
                // the old behaviour and still better than refusing the call.
                callTask(self, &conn, stream);
            };
        }

        try calls.await(self.io);
    }

    fn register(self: *Server, conn: *h2.Connection) !void {
        self.registry_mutex.lockUncancelable(self.io);
        defer self.registry_mutex.unlock(self.io);
        try self.registry.append(self.allocator, conn);
    }

    fn unregister(self: *Server, conn: *h2.Connection) void {
        self.registry_mutex.lockUncancelable(self.io);
        defer self.registry_mutex.unlock(self.io);
        for (self.registry.items, 0..) |c, i| {
            if (c == conn) {
                _ = self.registry.swapRemove(i);
                return;
            }
        }
    }

    /// One call. Errors are logged rather than returned: a call is not what
    /// the connection is waiting on, and one bad call must not close it.
    fn callTask(self: *Server, conn: *h2.Connection, stream: *h2.Stream) void {
        defer conn.closeStream(stream);

        self.serveCall(conn, stream) catch |err| switch (err) {
            // The ordinary ends of a call: the client cancelled, hung up, or
            // the server is shutting down.
            error.StreamReset,
            error.ConnectionClosed,
            error.EndOfStream,
            error.Canceled,
            error.DeadlineExceeded,
            => {},
            else => log.warn("call failed: {t}", .{err}),
        };
    }

    // `anyerror` so the disconnect cases above can be named explicitly; the
    // inferred set varies with the Io implementation in use.
    fn serveCall(self: *Server, conn: *h2.Connection, stream: *h2.Stream) anyerror!void {
        var call_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer call_arena.deinit();

        // Request bytes are read straight into the framing decoder's buffer,
        // so they are copied once on the way from the transport rather than
        // staged in between.
        var decoder = framing.Decoder.init(self.allocator);
        decoder.max_message_bytes = self.options.max_message_bytes;
        defer decoder.deinit();

        var headers = switch (try stream.next(try decoder.reserve(recv_chunk))) {
            .headers => |h| h,
            // A stream that opens with DATA, or with nothing at all, never
            // named a method; there is no way to answer it.
            .data => return error.UnexpectedData,
            .end => return,
        };
        defer headers.deinit();

        const path = headers.get(":path") orelse {
            try self.respondStatusOnly(conn, stream.id, .{
                .code = .internal,
                .message = "missing :path",
            });
            return;
        };

        // The clock starts here, on the first thing the server does with the
        // call, so that everything after it is inside the budget.
        const deadline: ?std.Io.Clock.Timestamp = deadline: {
            const text = headers.get("grpc-timeout") orelse break :deadline null;
            // A `grpc-timeout` we cannot parse is not a reason to fail the
            // call; it is a reason to treat it as unbounded, which is what a
            // client that sent no timeout would have got.
            const ns = timeout.decode(text) catch break :deadline null;
            break :deadline .fromNow(self.io, .{
                .raw = .fromNanoseconds(@intCast(ns)),
                .clock = deadline_clock,
            });
        };
        stream.setDeadline(deadline);

        // END_STREAM on the HEADERS frame means a zero-length request, which
        // is legal.
        var request_bytes: []const u8 = "";
        if (!headers.end_stream) {
            request_bytes = self.readRequestMessage(stream, &decoder, call_arena.allocator()) catch |err| {
                try self.respondStatusOnly(conn, stream.id, .{
                    .code = switch (err) {
                        error.MessageTooLarge => .resource_exhausted,
                        error.DeadlineExceeded => .deadline_exceeded,
                        else => .internal,
                    },
                    .message = @errorName(err),
                });
                return;
            };
        }

        const handler = self.lookup(path) orelse {
            try self.respondStatusOnly(conn, stream.id, .{
                .code = .unimplemented,
                .message = "method not found",
            });
            return;
        };

        var ctx: CallContext = .{
            .allocator = call_arena.allocator(),
            .io = self.io,
            .path = path,
            .deadline = deadline,
            .request_headers = &headers,
        };

        switch (handler) {
            .unary => |call| {
                const response = call(&ctx, request_bytes) catch |err| {
                    try self.respondStatusOnly(conn, stream.id, failureStatus(&ctx, err));
                    return;
                };

                if (!ctx.status.isOk()) {
                    try self.respondStatusOnly(conn, stream.id, ctx.status);
                    return;
                }

                // A handler that overran its deadline has produced an answer
                // nobody is waiting for: the client's own clock fired first,
                // since it started earlier. Say so rather than pretend.
                if (ctx.expired()) {
                    try self.respondStatusOnly(conn, stream.id, deadline_status);
                    return;
                }

                try self.respond(conn, stream, response, Status.ok);
            },

            .server_stream => |call| {
                var out: ServerStream = .{
                    .allocator = self.allocator,
                    .conn = conn,
                    .stream = stream,
                };
                defer out.deinit();

                if (call(&ctx, request_bytes, &out)) |_| {
                    const final = if (ctx.expired() and ctx.status.isOk()) deadline_status else ctx.status;
                    try self.finishStream(conn, stream, &out, final);
                } else |err| {
                    // A client that cancelled has already torn the stream down.
                    // There is nothing to report to and nothing to report — it
                    // is a normal end to the call, not a failure.
                    if (err == error.StreamReset or stream.isReset()) return;

                    // Once any message has gone out the response headers are
                    // already on the wire, so the failure has to be reported in
                    // trailers rather than as a Trailers-Only response.
                    try self.finishStream(conn, stream, &out, failureStatus(&ctx, err));
                }
            },
        }
    }

    fn finishStream(
        self: *Server,
        conn: *h2.Connection,
        stream: *h2.Stream,
        out: *ServerStream,
        call_status: Status,
    ) !void {
        if (!out.headers_sent) {
            // A stream that produced nothing collapses to Trailers-Only, which
            // is legal for both an empty success and an early failure.
            try self.respondStatusOnly(conn, stream.id, call_status);
            return;
        }
        try self.writeTrailers(conn, stream.id, call_status);
        try conn.flush();
    }

    fn readRequestMessage(
        self: *Server,
        stream: *h2.Stream,
        decoder: *framing.Decoder,
        arena: std.mem.Allocator,
    ) ![]const u8 {
        _ = self;
        while (true) {
            switch (try stream.next(try decoder.reserve(recv_chunk))) {
                .data => |n| {
                    decoder.commit(n);
                    if (try decoder.next()) |msg| {
                        if (msg.compressed) return error.CompressionNotSupported;
                        // Copied out: the decoder's buffer is reused, and the
                        // handler may hold this for the whole call.
                        return try arena.dupe(u8, msg.payload);
                    }
                },
                // Trailing HEADERS from the client ends the request stream.
                .headers => |h| {
                    var trailers = h;
                    trailers.deinit();
                },
                .end => {
                    // A stream that ends mid-message was truncated; reporting
                    // it as an empty request would silently lose data.
                    if (decoder.isDrained()) return "";
                    return error.TruncatedMessage;
                },
            }
        }
    }

    const deadline_status: Status = .{ .code = .deadline_exceeded, .message = "deadline exceeded" };

    /// A handler that set an explicit status keeps it. A deadline is not an
    /// unexpected failure and gets its own code; anything else maps to
    /// INTERNAL.
    fn failureStatus(ctx: *const CallContext, err: anyerror) Status {
        if (!ctx.status.isOk()) return ctx.status;
        if (err == error.DeadlineExceeded) return deadline_status;
        return .{ .code = .internal, .message = @errorName(err) };
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
        stream: *h2.Stream,
        message: []const u8,
        call_status: Status,
    ) !void {
        try conn.writeHeaders(stream.id, &.{
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

        try conn.writeData(stream, body.items, false);
        try self.writeTrailers(conn, stream.id, call_status);
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
