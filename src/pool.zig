//! A pool of persistent connections to one peer.
//!
//! What this is for, given that HTTP/2 already multiplexes: a single
//! connection is the unit of two limits. The peer's
//! `SETTINGS_MAX_CONCURRENT_STREAMS` caps how many calls can be in flight on
//! it, and one connection's frames are parsed by one reader task, so at a high
//! enough call rate that task saturates a core. A pool is how you get past
//! either. It is also where connection lifecycle lives: a connection whose
//! peer has gone away or said GOAWAY is retired and replaced, which a bare
//! `Client` will not do for you.
//!
//! What this is not: load balancing. Every connection goes to the same
//! address. Spreading calls over several peers needs name resolution and a
//! picker, and nothing here needs that yet.
//!
//! Connections are grown lazily up to `Options.size`. A call takes an idle
//! connection if there is one, and only opens another when every existing
//! connection is already busy — so a client that issues one call at a time
//! keeps exactly one connection however large the pool is allowed to be.

const std = @import("std");
const client_mod = @import("client.zig");
const status = @import("status.zig");

const Client = client_mod.Client;
const Response = client_mod.Response;

const log = std.log.scoped(.zgrpc);

pub const Options = struct {
    /// Ceiling on connections to the peer.
    ///
    /// A ceiling, not a target — connections are opened only when every
    /// existing one is busy, so a caller that issues one call at a time holds
    /// exactly one connection whatever this is set to. That is why the default
    /// is not 1: raising the ceiling costs nothing until there is concurrency
    /// to justify it.
    ///
    /// Four is where the loopback benchmark stops improving on a 4-core
    /// machine (`zig build bench`, "under load"): against a single connection
    /// it was worth about 30% more throughput and 30% off p50 at both 8 and 64
    /// concurrent callers. The number is a property of the machine — one
    /// connection's frames are parsed by one reader task, so the useful
    /// ceiling tracks cores — and is worth re-measuring on the target host.
    size: usize = 4,
    /// Applied to every connection the pool opens.
    client: client_mod.Options = .{},
};

/// One pooled connection, and the calls using it.
const Entry = struct {
    client: *Client,
    /// Calls currently holding this connection. A retired entry is destroyed
    /// when this reaches zero, which is what keeps a connection alive under a
    /// server-streaming call that outlives the decision to replace it.
    refs: usize = 0,
    /// Set once the connection must take no new calls.
    retired: bool = false,
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    /// Borrowed; must outlive the pool.
    authority: []const u8,
    options: Options,

    mutex: std.Io.Mutex = .init,
    /// One per connection the pool may hold. Null while empty or being
    /// connected.
    slots: []?*Entry,
    /// Whether a slot has a connect attempt in progress, so two callers do not
    /// race to fill the same one.
    connecting: []bool,
    /// Retired connections still finishing their calls. Off the slots, so a
    /// replacement can be opened straight away.
    draining: std.ArrayList(*Entry) = .empty,

    /// Signalled when a slot is filled, or a connect attempt gives up.
    slot_ready: std.Io.Condition = .init,
    /// Signalled when a call releases a connection. `deinit` waits on it.
    call_done: std.Io.Condition = .init,

    closed: bool = false,

    /// Opens the pool and its first connection.
    ///
    /// The first connection is established here rather than on the first call:
    /// a peer that is not there is better reported at construction than at the
    /// first query, and it keeps the handshake off the latency of that query.
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        address: std.Io.net.IpAddress,
        authority: []const u8,
        options: Options,
    ) !*Pool {
        std.debug.assert(options.size >= 1);

        const self = try allocator.create(Pool);
        errdefer allocator.destroy(self);

        const slots = try allocator.alloc(?*Entry, options.size);
        errdefer allocator.free(slots);
        @memset(slots, null);

        const connecting = try allocator.alloc(bool, options.size);
        errdefer allocator.free(connecting);
        @memset(connecting, false);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .address = address,
            .authority = authority,
            .options = options,
            .slots = slots,
            .connecting = connecting,
        };

        const first = try allocator.create(Entry);
        errdefer allocator.destroy(first);
        first.* = .{ .client = try Client.connect(allocator, io, address, authority, options.client) };
        slots[0] = first;

        return self;
    }

    /// Closes every connection.
    ///
    /// Waits for calls in flight first: tearing a connection down under a call
    /// still reading from it is a use-after-free, and a shutdown that blocks
    /// is a better failure than one that corrupts. Callers should stop issuing
    /// calls before getting here.
    pub fn deinit(self: *Pool) void {
        const io = self.io;

        var doomed: std.ArrayList(*Entry) = .empty;
        defer doomed.deinit(self.allocator);

        {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);

            self.closed = true;
            // Wakes anyone parked waiting for a slot, who then sees `closed`.
            self.slot_ready.broadcast(io);

            // Connects in progress count as busy: one is about to touch
            // `slots`, and finishing the teardown underneath it would free the
            // array out from under it.
            while (self.inFlightLocked() > 0 or self.connectingLocked()) {
                self.call_done.waitUncancelable(io, &self.mutex);
            }

            for (self.slots) |*slot| {
                if (slot.*) |entry| doomed.append(self.allocator, entry) catch {};
                slot.* = null;
            }
            for (self.draining.items) |entry| doomed.append(self.allocator, entry) catch {};
            self.draining.clearRetainingCapacity();
        }

        // Outside the lock: `Client.deinit` writes GOAWAY and closes a socket.
        for (doomed.items) |entry| self.destroy(entry);

        self.draining.deinit(self.allocator);
        self.allocator.free(self.slots);
        self.allocator.free(self.connecting);
        self.allocator.destroy(self);
    }

    /// Performs one unary call on some connection of the pool.
    pub fn call(
        self: *Pool,
        path: []const u8,
        request: []const u8,
        call_options: client_mod.CallOptions,
    ) !Response {
        const entry = try self.acquire();
        defer self.release(entry);
        return entry.client.call(path, request, call_options);
    }

    /// Starts a server-streaming call. The connection is held until the
    /// returned stream is deinited.
    pub fn callServerStream(
        self: *Pool,
        path: []const u8,
        request: []const u8,
        call_options: client_mod.CallOptions,
    ) !PooledStream {
        const entry = try self.acquire();
        errdefer self.release(entry);

        return .{
            .pool = self,
            .entry = entry,
            .stream = try entry.client.callServerStream(path, request, call_options),
        };
    }

    /// Connections currently open, for tests and for anyone sizing a pool.
    pub fn connectionCount(self: *Pool) usize {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var n: usize = 0;
        for (self.slots) |slot| {
            if (slot != null) n += 1;
        }
        return n + self.draining.items.len;
    }

    // -- internals ---------------------------------------------------------

    /// Takes a connection, opening one if that is the right thing to do.
    ///
    /// The policy: reuse an idle connection; failing that, grow; failing that,
    /// share the least loaded one. Growing before sharing matters because the
    /// reason to have a second connection at all is that the first is busy.
    fn acquire(self: *Pool) !*Entry {
        const io = self.io;

        while (true) {
            var chosen: ?*Entry = null;
            var connect_slot: ?usize = null;
            var reapable = false;

            {
                try self.mutex.lock(io);
                defer self.mutex.unlock(io);

                if (self.closed) return error.PoolClosed;
                self.retireDeadLocked();

                var least: ?*Entry = null;
                for (self.slots) |slot| {
                    const entry = slot orelse continue;
                    if (least == null or entry.refs < least.?.refs) least = entry;
                }

                if (least != null and least.?.refs == 0) {
                    // An idle connection is always the right answer: there is
                    // nothing for a new one to relieve.
                    least.?.refs = 1;
                    chosen = least;
                } else {
                    const empty: ?usize = for (self.slots, 0..) |slot, i| {
                        if (slot == null and !self.connecting[i]) break i;
                    } else null;

                    if (empty) |i| {
                        self.connecting[i] = true;
                        connect_slot = i;
                    } else if (least) |entry| {
                        // At capacity: share the least loaded connection.
                        entry.refs += 1;
                        chosen = entry;
                    } else {
                        // Every slot is mid-connect and none has landed yet.
                        try self.slot_ready.wait(io, &self.mutex);
                    }
                }

                reapable = self.draining.items.len > 0;
            }

            // Outside the lock: closing a connection is I/O.
            if (reapable) self.reap();

            if (chosen) |entry| return entry;
            if (connect_slot) |i| return self.fill(i);
        }
    }

    /// Connects and installs the connection for `index`, which this caller has
    /// already claimed by setting `connecting`.
    fn fill(self: *Pool, index: usize) !*Entry {
        const io = self.io;

        const connected = Client.connect(
            self.allocator,
            self.io,
            self.address,
            self.authority,
            self.options.client,
        ) catch |err| {
            self.abandon(index);
            return err;
        };
        errdefer connected.deinit();

        const entry = self.allocator.create(Entry) catch |err| {
            self.abandon(index);
            return err;
        };
        entry.* = .{ .client = connected, .refs = 1 };
        errdefer self.allocator.destroy(entry);

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        // The pool was closed while this connect was in flight, and `deinit`
        // is waiting for exactly this. Hand nothing back and let it finish.
        if (self.closed) {
            self.connecting[index] = false;
            self.slot_ready.broadcast(io);
            self.call_done.broadcast(io);
            return error.PoolClosed;
        }

        self.slots[index] = entry;
        self.connecting[index] = false;
        self.slot_ready.broadcast(io);
        return entry;
    }

    /// Gives up a slot this caller had claimed, after a failed connect.
    fn abandon(self: *Pool, index: usize) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.connecting[index] = false;
        self.slot_ready.broadcast(io);
        self.call_done.broadcast(io);
    }

    fn release(self: *Pool, entry: *Entry) void {
        const io = self.io;

        const reapable = blk: {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);

            entry.refs -= 1;
            self.call_done.broadcast(io);
            break :blk self.draining.items.len > 0;
        };

        // Nothing draining is the common case, and it costs one comparison to
        // keep the second lock acquisition off the path a call actually takes.
        if (reapable) self.reap();
    }

    /// Closes retired connections whose last call has finished.
    ///
    /// Separate from retirement because tearing a connection down writes
    /// GOAWAY and closes a socket, and the pool lock must not be held across
    /// either.
    fn reap(self: *Pool) void {
        const io = self.io;

        while (true) {
            const entry = blk: {
                self.mutex.lockUncancelable(io);
                defer self.mutex.unlock(io);

                for (self.draining.items, 0..) |candidate, i| {
                    if (candidate.refs == 0) break :blk self.draining.orderedRemove(i);
                }
                break :blk null;
            } orelse return;

            self.destroy(entry);
        }
    }

    /// Moves connections the peer has finished with off the slots, so the next
    /// call opens a replacement instead of failing on a dead one. Caller must
    /// hold `mutex`.
    fn retireDeadLocked(self: *Pool) void {
        for (self.slots) |*slot| {
            const entry = slot.* orelse continue;
            if (entry.client.isUsable()) continue;

            entry.retired = true;
            slot.* = null;
            // Calls may still be finishing on it, and closing it is I/O either
            // way, so `reap` disposes of it once its last call is done.
            self.draining.append(self.allocator, entry) catch {
                // The list could not grow. Leaving the entry unreferenced
                // would leak it, so keep it in its slot and try again next
                // time — a dead connection in a slot costs one failed call.
                entry.retired = false;
                slot.* = entry;
            };
        }
    }

    fn connectingLocked(self: *Pool) bool {
        for (self.connecting) |busy| {
            if (busy) return true;
        }
        return false;
    }

    fn inFlightLocked(self: *Pool) usize {
        var n: usize = 0;
        for (self.slots) |slot| {
            if (slot) |entry| n += entry.refs;
        }
        for (self.draining.items) |entry| n += entry.refs;
        return n;
    }

    fn destroy(self: *Pool, entry: *Entry) void {
        entry.client.deinit();
        self.allocator.destroy(entry);
    }
};

/// A server-streaming call on a pooled connection.
///
/// Holds the connection until `deinit`, which is the only reason this wraps
/// `client.ResponseStream` rather than returning it directly: the pool must
/// not retire a connection out from under a stream still reading it.
pub const PooledStream = struct {
    pool: *Pool,
    entry: *Entry,
    stream: client_mod.ResponseStream,

    pub fn deinit(self: *PooledStream) void {
        self.stream.deinit();
        self.pool.release(self.entry);
    }

    /// Next message, or null once the server has finished. Same borrowing
    /// rules as `client.ResponseStream.next`: valid until the following call.
    pub fn next(self: *PooledStream) !?[]const u8 {
        return self.stream.next();
    }

    pub fn finalStatus(self: *const PooledStream) status.Status {
        return self.stream.finalStatus();
    }
};
