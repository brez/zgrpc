//! gRPC status codes and the status carried in `grpc-status` / `grpc-message`
//! response trailers.
//!
//! Codes are the canonical set from grpc/status.h; the numeric values are wire
//! format and must not be renumbered.

const std = @import("std");

pub const Code = enum(u32) {
    ok = 0,
    cancelled = 1,
    unknown = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,

    /// Parses the decimal value of a `grpc-status` trailer.
    ///
    /// An unrecognised numeric code maps to `.unknown` rather than erroring:
    /// a peer using a code we do not know about is still reporting a failure,
    /// and dropping the call for that reason would be worse than reporting it
    /// imprecisely.
    pub fn fromTrailer(text: []const u8) !Code {
        const raw = std.fmt.parseInt(u32, text, 10) catch return error.InvalidStatusCode;
        return fromInt(raw);
    }

    pub fn fromInt(raw: u32) Code {
        return std.enums.fromInt(Code, raw) orelse .unknown;
    }

    pub fn toInt(self: Code) u32 {
        return @intFromEnum(self);
    }
};

/// A complete gRPC call outcome: the code plus the optional `grpc-message`.
///
/// `message` is borrowed, not owned — it points into the caller's trailer
/// buffer. Callers that need it to outlive the call must copy it.
pub const Status = struct {
    code: Code,
    message: []const u8 = "",

    pub const ok: Status = .{ .code = .ok };

    pub fn isOk(self: Status) bool {
        return self.code == .ok;
    }
};

/// `grpc-message` is percent-encoded on the wire (RFC 3986-style) because
/// HTTP/2 header values may not carry arbitrary bytes. Only ASCII letters,
/// digits and a small punctuation set survive unescaped; everything else,
/// including all non-ASCII UTF-8, is emitted as %XX per byte.
fn isMessageSafe(c: u8) bool {
    return switch (c) {
        'a'...'z', 'A'...'Z', '0'...'9' => true,
        ' ', '-', '_', '.', '~' => true,
        else => false,
    };
}

pub fn encodeMessage(allocator: std.mem.Allocator, message: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (message) |c| {
        if (isMessageSafe(c)) {
            try out.append(allocator, c);
        } else {
            try out.print(allocator, "%{X:0>2}", .{c});
        }
    }
    return out.toOwnedSlice(allocator);
}

pub fn decodeMessage(allocator: std.mem.Allocator, encoded: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < encoded.len) {
        if (encoded[i] == '%' and i + 2 < encoded.len) {
            const hi = std.fmt.charToDigit(encoded[i + 1], 16) catch {
                try out.append(allocator, encoded[i]);
                i += 1;
                continue;
            };
            const lo = std.fmt.charToDigit(encoded[i + 2], 16) catch {
                try out.append(allocator, encoded[i]);
                i += 1;
                continue;
            };
            try out.append(allocator, hi * 16 + lo);
            i += 3;
        } else {
            try out.append(allocator, encoded[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

test "status code parses from trailer text" {
    try std.testing.expectEqual(Code.ok, try Code.fromTrailer("0"));
    try std.testing.expectEqual(Code.not_found, try Code.fromTrailer("5"));
    try std.testing.expectEqual(Code.unauthenticated, try Code.fromTrailer("16"));
}

test "unknown numeric status degrades to .unknown rather than failing the call" {
    try std.testing.expectEqual(Code.unknown, try Code.fromTrailer("99"));
}

test "non-numeric grpc-status is a protocol error" {
    try std.testing.expectError(error.InvalidStatusCode, Code.fromTrailer("nope"));
    try std.testing.expectError(error.InvalidStatusCode, Code.fromTrailer(""));
}

test "grpc-message percent-encoding round-trips non-ascii" {
    const alloc = std.testing.allocator;
    const original = "keine Verbindung: ünïcode ✓";

    const encoded = try encodeMessage(alloc, original);
    defer alloc.free(encoded);
    // Every byte in the encoded form must be header-safe.
    for (encoded) |c| try std.testing.expect(isMessageSafe(c) or c == '%');

    const decoded = try decodeMessage(alloc, encoded);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(original, decoded);
}

test "grpc-message encoding leaves safe text untouched" {
    const alloc = std.testing.allocator;
    const encoded = try encodeMessage(alloc, "not found");
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings("not found", encoded);
}
