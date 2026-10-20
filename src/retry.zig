//! Deciding whether, and when, to send a failed call again.
//!
//! Kept apart from the pool that uses it so that every rule here can be tested
//! with scripted outcomes and no sockets. The pool supplies one attempt at a
//! time; this decides what happens after it fails.
//!
//! Two kinds of retry, with different safety:
//!
//!   - *Unprocessed.* The call provably never ran (`error.Unprocessed`: the
//!     peer refused the stream, or it sat past a GOAWAY, or the connection was
//!     already closed when the call began). Sending it again cannot run it
//!     twice, so this needs no permission from the caller, and it is immediate:
//!     there is nothing to back off from.
//!
//!   - *Failed.* The call may have run: the server answered UNAVAILABLE, or the
//!     connection died mid-call. Sending it again can run it twice, so it
//!     happens only if the caller said the call is `idempotent`. These back
//!     off, are throttled, and stop at the call's deadline.
//!
//! Anything else (a deadline, a bad argument, a status the policy does not name)
//! is the answer, and is returned as it came.

const std = @import("std");
const Code = @import("status.zig").Code;

pub const CodeSet = std.EnumSet(Code);

pub const Policy = struct {
    /// Total attempts at a call that may have run, the first included. 1 turns
    /// this kind of retry off. Applies only to `idempotent` calls.
    max_attempts: u32 = 3,
    /// The wait before the first retry. Each further retry doubles it, up to
    /// `max_backoff_ns`, and the wait actually taken is a random fraction of
    /// that, so callers that failed together do not retry together.
    initial_backoff_ns: u64 = 50 * std.time.ns_per_ms,
    max_backoff_ns: u64 = 2 * std.time.ns_per_s,
    /// The statuses worth another try. UNAVAILABLE is "the server could not be
    /// reached or is going away", which is the case retrying is for;
    /// RESOURCE_EXHAUSTED and the like are the server asking for less load.
    retryable: CodeSet = CodeSet.initMany(&.{.unavailable}),
    /// How many times to resend a call that provably never ran. A cap rather
    /// than a loop only so that a pool where every connection refuses cannot
    /// spin.
    max_unprocessed: u32 = 2,
    /// Limits retries to what the server seems able to absorb; see `Throttle`.
    /// Null retries whenever the rules above allow.
    throttle: ?Throttle.Config = .{},
};

/// Stops retries from piling onto a server that is already struggling, which
/// is when a retry policy does the most harm. The same scheme as gRPC's:
/// a bucket of tokens, one lost to every call that fails with a retryable
/// status, a fraction regained by every success, and retries allowed only
/// while the bucket is more than half full.
///
/// So a server answering most calls loses nothing, and one failing most of
/// them quickly runs the bucket below the line and stops being retried against,
/// until it recovers.
pub const Throttle = struct {
    pub const Config = struct {
        max_tokens: u32 = 10,
        /// Tokens regained per success.
        token_ratio: f32 = 0.1,
    };

    /// Thousandths of a token, so the arithmetic is atomic and exact.
    milli_tokens: std.atomic.Value(u32),
    max_milli: u32,
    ratio_milli: u32,

    pub fn init(config: Config) Throttle {
        const max_milli = config.max_tokens * 1000;
        return .{
            .milli_tokens = .init(max_milli),
            .max_milli = max_milli,
            .ratio_milli = @intFromFloat(config.token_ratio * 1000),
        };
    }

    pub fn onSuccess(self: *Throttle) void {
        var current = self.milli_tokens.load(.monotonic);
        while (true) {
            const next = @min(self.max_milli, current +| self.ratio_milli);
            current = self.milli_tokens.cmpxchgWeak(current, next, .monotonic, .monotonic) orelse return;
        }
    }

    pub fn onFailure(self: *Throttle) void {
        var current = self.milli_tokens.load(.monotonic);
        while (true) {
            const next = current -| 1000;
            current = self.milli_tokens.cmpxchgWeak(current, next, .monotonic, .monotonic) orelse return;
        }
    }

    pub fn allowsRetry(self: *const Throttle) bool {
        return self.milli_tokens.load(.monotonic) > self.max_milli / 2;
    }
};

/// Whether an error from an attempt leaves it unknown if the server acted on
/// the call, and is the sort that goes away on its own: a connection that
/// dropped or a stream the peer reset. Deadlines, cancellation, a call the
/// client refused to build, and running out of memory are not.
pub fn isRetryableFailure(err: anyerror) bool {
    return switch (err) {
        error.StreamReset,
        error.ConnectionClosed,
        error.ConnectionResetByPeer,
        error.EndOfStream,
        error.BrokenPipe,
        error.KeepaliveTimeout,
        error.MissingStatus,
        => true,
        else => false,
    };
}

/// The wait before retry number `retry` (1 for the first), before jitter.
pub fn backoffCeiling(policy: Policy, retry: u32) u64 {
    var ceiling = policy.initial_backoff_ns;
    var i: u32 = 1;
    while (i < retry and ceiling < policy.max_backoff_ns) : (i += 1) ceiling *|= 2;
    return @min(ceiling, policy.max_backoff_ns);
}

/// Drives attempts until one is final, and returns it.
///
/// `attempts` is what the caller supplies, three functions over the attempt's
/// result type `T`:
///
///     fn attempt(self: *@This(), remaining_ns: ?u64) anyerror!T
///     fn code(value: *const T) ?Code    // its status, if it has one yet
///     fn discard(value: *T) void        // releases a result being thrown away
///
/// `deadline_ns` bounds the whole sequence, not each attempt: every attempt is
/// told how much of it is left, and no wait is taken that would run past it.
pub fn run(
    comptime T: type,
    io: std.Io,
    policy: Policy,
    throttle: ?*Throttle,
    idempotent: bool,
    deadline_ns: ?u64,
    attempts: anytype,
) anyerror!T {
    const started = if (deadline_ns != null) std.Io.Timestamp.now(io, .awake).nanoseconds else 0;
    var unprocessed: u32 = 0;
    var failures: u32 = 0;

    while (true) {
        var remaining: ?u64 = null;
        if (deadline_ns) |total| {
            const elapsed: u64 = @intCast(@max(0, std.Io.Timestamp.now(io, .awake).nanoseconds - started));
            if (elapsed >= total) return error.DeadlineExceeded;
            remaining = total - elapsed;
        }

        // What this attempt came to, as something to retry or something to hand
        // back. Held until the decision is made, because a result that is about
        // to be thrown away must be discarded and one that is returned must not.
        var outcome = attempts.attempt(remaining);

        if (outcome) |*value| {
            const code = attempts.code(value) orelse return value.*;
            if (code == .ok) {
                if (throttle) |t| t.onSuccess();
                return value.*;
            }
            if (!policy.retryable.contains(code)) return value.*;
            if (throttle) |t| t.onFailure();
            if (!idempotent) return value.*;
            if (!worthAnother(policy, throttle, failures + 1)) return value.*;
        } else |err| {
            if (err == error.Unprocessed) {
                if (unprocessed >= policy.max_unprocessed) return err;
                unprocessed += 1;
                continue;
            }
            if (!idempotent or !isRetryableFailure(err)) return err;
            if (throttle) |t| t.onFailure();
            if (!worthAnother(policy, throttle, failures + 1)) return err;
        }

        failures += 1;
        const wait = jitter(io, backoffCeiling(policy, failures));
        if (deadline_ns) |total| {
            const elapsed: u64 = @intCast(@max(0, std.Io.Timestamp.now(io, .awake).nanoseconds - started));
            // Waiting out the deadline buys nothing: the next attempt would be
            // told it has no time. Better to return what we have, which is
            // why a result is only discarded once the retry is certain.
            if (elapsed + wait >= total) return outcome;
        }
        if (outcome) |*value| attempts.discard(value) else |_| {}
        try io.sleep(.fromNanoseconds(@intCast(wait)), .awake);
    }
}

/// Attempts used so far, `failed` of them, against what the policy allows.
fn worthAnother(policy: Policy, throttle: ?*Throttle, failed: u32) bool {
    if (failed >= policy.max_attempts) return false;
    if (throttle) |t| if (!t.allowsRetry()) return false;
    return true;
}

fn jitter(io: std.Io, ceiling: u64) u64 {
    if (ceiling == 0) return 0;
    var bytes: [8]u8 = undefined;
    io.random(&bytes);
    return std.mem.readInt(u64, &bytes, .little) % (ceiling + 1);
}

// -- tests -------------------------------------------------------------------

const testing = std.testing;

/// A result that owns memory, so a test notices one that is discarded twice or
/// not at all.
const Reply = struct {
    status: Code,
    note: []u8,
};

/// Plays back a script of outcomes, and records what was asked of it.
const Script = struct {
    steps: []const Step,
    next: usize = 0,
    asked: u32 = 0,
    last_remaining: ?u64 = null,
    discarded: u32 = 0,

    const Step = union(enum) {
        status: Code,
        err: anyerror,
    };

    fn attempt(self: *Script, remaining_ns: ?u64) anyerror!Reply {
        self.asked += 1;
        self.last_remaining = remaining_ns;
        const step = self.steps[@min(self.next, self.steps.len - 1)];
        self.next += 1;
        switch (step) {
            .status => |status_code| return .{ .status = status_code, .note = try testing.allocator.alloc(u8, 4) },
            .err => |e| return e,
        }
    }

    fn code(_: *Script, value: *const Reply) ?Code {
        return value.status;
    }

    fn discard(self: *Script, value: *Reply) void {
        self.discarded += 1;
        testing.allocator.free(value.note);
    }
};

/// Fast, so the tests wait microseconds rather than the production defaults.
const quick: Policy = .{ .initial_backoff_ns = 1000, .max_backoff_ns = 4000 };

fn runScript(script: *Script, policy: Policy, throttle: ?*Throttle, idempotent: bool, deadline_ns: ?u64) anyerror!Reply {
    return run(Reply, testing.io, policy, throttle, idempotent, deadline_ns, script);
}

test "an idempotent call is retried on UNAVAILABLE until it succeeds" {
    var script: Script = .{ .steps = &.{ .{ .status = .unavailable }, .{ .status = .unavailable }, .{ .status = .ok } } };
    const reply = try runScript(&script, quick, null, true, null);
    defer testing.allocator.free(reply.note);

    try testing.expectEqual(Code.ok, reply.status);
    try testing.expectEqual(@as(u32, 3), script.asked);
    // The two failures were thrown away, and only those.
    try testing.expectEqual(@as(u32, 2), script.discarded);
}

test "a call that may have run is not retried unless it is idempotent" {
    var script: Script = .{ .steps = &.{ .{ .status = .unavailable }, .{ .status = .ok } } };
    const reply = try runScript(&script, quick, null, false, null);
    defer testing.allocator.free(reply.note);

    try testing.expectEqual(Code.unavailable, reply.status);
    try testing.expectEqual(@as(u32, 1), script.asked);
    try testing.expectEqual(@as(u32, 0), script.discarded);
}

test "a call that provably never ran is resent whether or not it is idempotent" {
    var script: Script = .{ .steps = &.{ .{ .err = error.Unprocessed }, .{ .status = .ok } } };
    const reply = try runScript(&script, quick, null, false, null);
    defer testing.allocator.free(reply.note);

    try testing.expectEqual(Code.ok, reply.status);
    try testing.expectEqual(@as(u32, 2), script.asked);
}

test "resending a call that never ran stops at the cap, and returns the error" {
    var script: Script = .{ .steps = &.{.{ .err = error.Unprocessed }} };
    try testing.expectError(error.Unprocessed, runScript(&script, quick, null, false, null));
    // The first attempt and `max_unprocessed` resends.
    try testing.expectEqual(@as(u32, 1 + quick.max_unprocessed), script.asked);
}

test "statuses the policy does not name are the answer, however idempotent" {
    for ([_]Code{ .invalid_argument, .not_found, .internal, .deadline_exceeded, .resource_exhausted }) |code| {
        var script: Script = .{ .steps = &.{ .{ .status = code }, .{ .status = .ok } } };
        const reply = try runScript(&script, quick, null, true, null);
        defer testing.allocator.free(reply.note);
        try testing.expectEqual(code, reply.status);
        try testing.expectEqual(@as(u32, 1), script.asked);
    }
}

test "a connection lost mid-call is retried only when idempotent" {
    {
        var script: Script = .{ .steps = &.{ .{ .err = error.ConnectionResetByPeer }, .{ .status = .ok } } };
        const reply = try runScript(&script, quick, null, true, null);
        defer testing.allocator.free(reply.note);
        try testing.expectEqual(Code.ok, reply.status);
        try testing.expectEqual(@as(u32, 2), script.asked);
    }
    {
        var script: Script = .{ .steps = &.{ .{ .err = error.ConnectionResetByPeer }, .{ .status = .ok } } };
        try testing.expectError(error.ConnectionResetByPeer, runScript(&script, quick, null, false, null));
        try testing.expectEqual(@as(u32, 1), script.asked);
    }
}

test "errors that retrying cannot fix are returned at once" {
    for ([_]anyerror{ error.InvalidMetadata, error.DeadlineExceeded, error.Canceled, error.CompressionNotSupported, error.OutOfMemory }) |err| {
        var script: Script = .{ .steps = &.{ .{ .err = err }, .{ .status = .ok } } };
        try testing.expectError(err, runScript(&script, quick, null, true, null));
        try testing.expectEqual(@as(u32, 1), script.asked);
    }
}

test "attempts are capped, and the last failure is what the caller gets" {
    var script: Script = .{ .steps = &.{.{ .status = .unavailable }} };
    const reply = try runScript(&script, .{ .max_attempts = 3, .initial_backoff_ns = 1000 }, null, true, null);
    defer testing.allocator.free(reply.note);

    try testing.expectEqual(Code.unavailable, reply.status);
    try testing.expectEqual(@as(u32, 3), script.asked);
    // Two thrown away; the third is handed back, not discarded.
    try testing.expectEqual(@as(u32, 2), script.discarded);
}

test "max_attempts of 1 turns retrying of failed calls off" {
    var script: Script = .{ .steps = &.{ .{ .status = .unavailable }, .{ .status = .ok } } };
    const reply = try runScript(&script, .{ .max_attempts = 1 }, null, true, null);
    defer testing.allocator.free(reply.note);
    try testing.expectEqual(@as(u32, 1), script.asked);
}

test "the deadline bounds the whole sequence, not each attempt" {
    // A wait of up to an hour against a 10ms deadline: no retry fits (the odds
    // of a draw under 10ms are about one in a million), so the first failure is
    // returned rather than waited out.
    var script: Script = .{ .steps = &.{ .{ .status = .unavailable }, .{ .status = .ok } } };
    const hour = 3600 * std.time.ns_per_s;
    const slow: Policy = .{ .initial_backoff_ns = hour, .max_backoff_ns = hour };
    const reply = try runScript(&script, slow, null, true, 10 * std.time.ns_per_ms);
    defer testing.allocator.free(reply.note);

    try testing.expectEqual(Code.unavailable, reply.status);
    try testing.expectEqual(@as(u32, 1), script.asked);
    try testing.expectEqual(@as(u32, 0), script.discarded);
}

test "each attempt is told how much of the deadline is left" {
    var script: Script = .{ .steps = &.{ .{ .status = .unavailable }, .{ .status = .ok } } };
    const reply = try runScript(&script, quick, null, true, 5 * std.time.ns_per_s);
    defer testing.allocator.free(reply.note);

    try testing.expectEqual(@as(u32, 2), script.asked);
    try testing.expect(script.last_remaining.? < 5 * std.time.ns_per_s);
    try testing.expect(script.last_remaining.? > 4 * std.time.ns_per_s);
}

test "backoff doubles from the initial wait up to the cap" {
    const policy: Policy = .{ .initial_backoff_ns = 100, .max_backoff_ns = 700 };
    try testing.expectEqual(@as(u64, 100), backoffCeiling(policy, 1));
    try testing.expectEqual(@as(u64, 200), backoffCeiling(policy, 2));
    try testing.expectEqual(@as(u64, 400), backoffCeiling(policy, 3));
    try testing.expectEqual(@as(u64, 700), backoffCeiling(policy, 4));
    try testing.expectEqual(@as(u64, 700), backoffCeiling(policy, 40));
}

test "the throttle stops retries once failures outnumber successes" {
    var throttle: Throttle = .init(.{ .max_tokens = 10, .token_ratio = 0.1 });
    try testing.expect(throttle.allowsRetry());

    // Five failures empty the bucket to exactly half, which is not above it.
    for (0..5) |_| throttle.onFailure();
    try testing.expect(!throttle.allowsRetry());

    // A script that keeps failing is then not retried at all.
    var script: Script = .{ .steps = &.{.{ .status = .unavailable }} };
    const reply = try runScript(&script, quick, &throttle, true, null);
    defer testing.allocator.free(reply.note);
    try testing.expectEqual(@as(u32, 1), script.asked);

    // Successes refill it, a tenth of a token each.
    for (0..20) |_| throttle.onSuccess();
    try testing.expect(throttle.allowsRetry());
}

test "the throttle does not refill past its maximum" {
    var throttle: Throttle = .init(.{ .max_tokens = 4, .token_ratio = 1.0 });
    for (0..100) |_| throttle.onSuccess();
    for (0..2) |_| throttle.onFailure();
    // 4 - 2 = 2, which is not more than half of 4; had it overflowed upward,
    // two failures would not have been enough.
    try testing.expect(!throttle.allowsRetry());
}

test "failures drain the throttle even when the call is not retried" {
    var throttle: Throttle = .init(.{});
    var script: Script = .{ .steps = &.{.{ .status = .unavailable }} };
    for (0..6) |_| {
        const reply = try runScript(&script, quick, &throttle, false, null);
        testing.allocator.free(reply.note);
    }
    // A server failing every call stops being retried against even by callers
    // who are not yet retrying, so the bucket reflects the server, not the use.
    try testing.expect(!throttle.allowsRetry());
}
