//! `grpc-timeout` header encoding — how a client's deadline is propagated to
//! the server.
//!
//! Wire format is a positive integer of at most 8 digits followed by a unit
//! character: `H`ours, `M`inutes, `S`econds, `m`illi, `u`micro, `n`ano.
//! e.g. `100m` = 100 milliseconds.
//!
//! The 8-digit cap is a hard protocol limit, not a suggestion: a value needing
//! more digits must be expressed in a coarser unit, so encoding picks the
//! finest unit that both fits and loses no precision.

const std = @import("std");

pub const max_digits = 8;
const max_value: u64 = 99_999_999;

pub const Unit = enum {
    hours,
    minutes,
    seconds,
    milliseconds,
    microseconds,
    nanoseconds,

    pub fn char(self: Unit) u8 {
        return switch (self) {
            .hours => 'H',
            .minutes => 'M',
            .seconds => 'S',
            .milliseconds => 'm',
            .microseconds => 'u',
            .nanoseconds => 'n',
        };
    }

    pub fn fromChar(c: u8) ?Unit {
        return switch (c) {
            'H' => .hours,
            'M' => .minutes,
            'S' => .seconds,
            'm' => .milliseconds,
            'u' => .microseconds,
            'n' => .nanoseconds,
            else => null,
        };
    }

    pub fn nanosPerUnit(self: Unit) u64 {
        return switch (self) {
            .hours => 60 * 60 * std.time.ns_per_s,
            .minutes => 60 * std.time.ns_per_s,
            .seconds => std.time.ns_per_s,
            .milliseconds => std.time.ns_per_ms,
            .microseconds => std.time.ns_per_us,
            .nanoseconds => 1,
        };
    }
};

/// Coarsest unit first. Encoding prefers the coarsest *exact* representation,
/// which is both the most compact on the wire and what other gRPC
/// implementations emit — `100m`, not the equally-exact `100000u`.
const units_coarse_to_fine = [_]Unit{
    .hours,
    .minutes,
    .seconds,
    .milliseconds,
    .microseconds,
    .nanoseconds,
};

/// Formats a deadline into `buf`, returning the slice written.
///
/// `buf` need only be 9 bytes (8 digits + unit).
pub fn encode(buf: []u8, timeout_ns: u64) ![]const u8 {
    if (timeout_ns == 0) return error.TimeoutTooSmall;

    // Preferred: the coarsest unit that represents the deadline exactly and
    // fits the digit budget.
    for (units_coarse_to_fine) |unit| {
        const per = unit.nanosPerUnit();
        if (timeout_ns % per != 0) continue;
        const value = timeout_ns / per;
        if (value > max_value) continue;
        return std.fmt.bufPrint(buf, "{d}{c}", .{ value, unit.char() });
    }

    // Nothing divides evenly within 8 digits, so round *up* in the finest unit
    // that fits. Rounding up hands the server marginally more time than asked;
    // rounding down would cut a caller's deadline short, which is not safe.
    var i = units_coarse_to_fine.len;
    while (i > 0) {
        i -= 1;
        const unit = units_coarse_to_fine[i];
        const value = std.math.divCeil(u64, timeout_ns, unit.nanosPerUnit()) catch continue;
        if (value > max_value) continue;
        return std.fmt.bufPrint(buf, "{d}{c}", .{ value, unit.char() });
    }

    return error.TimeoutTooLarge;
}

/// Parses a `grpc-timeout` header value into nanoseconds.
pub fn decode(text: []const u8) !u64 {
    if (text.len < 2) return error.InvalidTimeout;
    const digits = text[0 .. text.len - 1];
    if (digits.len > max_digits) return error.InvalidTimeout;

    const unit = Unit.fromChar(text[text.len - 1]) orelse return error.InvalidTimeout;
    const value = std.fmt.parseInt(u64, digits, 10) catch return error.InvalidTimeout;

    return std.math.mul(u64, value, unit.nanosPerUnit()) catch error.TimeoutTooLarge;
}

test "encode picks the finest unit that represents the value exactly" {
    var buf: [16]u8 = undefined;
    try std.testing.expectEqualStrings("100m", try encode(&buf, 100 * std.time.ns_per_ms));
    try std.testing.expectEqualStrings("5S", try encode(&buf, 5 * std.time.ns_per_s));
    try std.testing.expectEqualStrings("1n", try encode(&buf, 1));
    try std.testing.expectEqualStrings("250u", try encode(&buf, 250 * std.time.ns_per_us));
}

test "encode falls back to a coarser unit rather than exceeding 8 digits" {
    var buf: [16]u8 = undefined;
    // 1 hour in nanoseconds needs 13 digits; as seconds it is 3600.
    const one_hour = 60 * 60 * std.time.ns_per_s;
    const out = try encode(&buf, one_hour);
    try std.testing.expect(out.len <= max_digits + 1);
    try std.testing.expectEqual(one_hour, try decode(out));
}

test "encode never silently shortens a deadline" {
    var buf: [16]u8 = undefined;
    // A value with no exact coarse representation must round up, not down.
    const awkward: u64 = 90 * 60 * std.time.ns_per_s + 1; // 90min + 1ns
    const out = try encode(&buf, awkward);
    try std.testing.expect(try decode(out) >= awkward);
}

test "encode output always fits the protocol digit limit" {
    var buf: [16]u8 = undefined;
    const cases = [_]u64{
        1,
        999,
        std.time.ns_per_ms,
        std.time.ns_per_s,
        60 * std.time.ns_per_s,
        24 * 60 * 60 * std.time.ns_per_s,
        std.math.maxInt(u32),
    };
    for (cases) |ns| {
        const out = try encode(&buf, ns);
        try std.testing.expect(out.len >= 2 and out.len <= max_digits + 1);
        try std.testing.expect(Unit.fromChar(out[out.len - 1]) != null);
    }
}

test "decode handles every unit character" {
    try std.testing.expectEqual(@as(u64, 1), try decode("1n"));
    try std.testing.expectEqual(@as(u64, std.time.ns_per_us), try decode("1u"));
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), try decode("1m"));
    try std.testing.expectEqual(@as(u64, std.time.ns_per_s), try decode("1S"));
    try std.testing.expectEqual(@as(u64, 60 * std.time.ns_per_s), try decode("1M"));
    try std.testing.expectEqual(@as(u64, 3600 * std.time.ns_per_s), try decode("1H"));
}

test "decode rejects malformed timeouts" {
    try std.testing.expectError(error.InvalidTimeout, decode(""));
    try std.testing.expectError(error.InvalidTimeout, decode("m"));
    try std.testing.expectError(error.InvalidTimeout, decode("100x"));
    try std.testing.expectError(error.InvalidTimeout, decode("abcm"));
    // More than 8 digits is a protocol violation.
    try std.testing.expectError(error.InvalidTimeout, decode("123456789m"));
}

test "encode/decode round-trip preserves common deadlines" {
    var buf: [16]u8 = undefined;
    const deadlines = [_]u64{
        5 * std.time.ns_per_ms, // the project's latency target
        100 * std.time.ns_per_ms,
        std.time.ns_per_s,
        30 * std.time.ns_per_s,
    };
    for (deadlines) |ns| {
        try std.testing.expectEqual(ns, try decode(try encode(&buf, ns)));
    }
}
