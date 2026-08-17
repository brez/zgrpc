//! gRPC length-prefixed message framing.
//!
//! Every gRPC message on the wire is:
//!
//!     +---------------+-----------------------+-------------------+
//!     | compressed: 1 | message length: 4 (BE)| payload: N bytes  |
//!     +---------------+-----------------------+-------------------+
//!
//! This framing sits *inside* the HTTP/2 DATA stream and is independent of
//! HTTP/2 frame boundaries: one gRPC message may span several DATA frames, and
//! one DATA frame may carry several gRPC messages. Nothing here may assume the
//! two align.

const std = @import("std");

pub const header_len = 5;

/// Guards against a malicious or buggy peer advertising a huge length and
/// making us allocate it before a single payload byte has arrived.
pub const default_max_message_bytes: u32 = 4 * 1024 * 1024;

pub const Header = struct {
    compressed: bool,
    length: u32,

    pub fn decode(bytes: *const [header_len]u8) !Header {
        return .{
            // Only 0 and 1 are defined; anything else means we have lost sync
            // with the stream, which is not recoverable by skipping.
            .compressed = switch (bytes[0]) {
                0 => false,
                1 => true,
                else => return error.InvalidCompressionFlag,
            },
            .length = std.mem.readInt(u32, bytes[1..5], .big),
        };
    }

    pub fn encode(self: Header) [header_len]u8 {
        var out: [header_len]u8 = undefined;
        out[0] = @intFromBool(self.compressed);
        std.mem.writeInt(u32, out[1..5], self.length, .big);
        return out;
    }
};

/// Writes one length-prefixed gRPC message.
///
/// Does not flush: the caller controls when bytes hit the socket, because for
/// unary calls the message and the trailing HEADERS frame should leave in as
/// few syscalls as possible.
pub fn writeMessage(writer: *std.Io.Writer, payload: []const u8) !void {
    if (payload.len > std.math.maxInt(u32)) return error.MessageTooLarge;
    const header: Header = .{ .compressed = false, .length = @intCast(payload.len) };
    try writer.writeAll(&header.encode());
    try writer.writeAll(payload);
}

/// Incremental de-framer that accepts arbitrary byte chunks as they arrive off
/// the HTTP/2 stream and yields whole messages.
///
/// Uses a read cursor rather than shifting the buffer down after every message,
/// so a DATA frame carrying many small messages is walked without repeatedly
/// memmoving the remainder. Consumed space is reclaimed on the next `push`.
pub const Decoder = struct {
    allocator: std.mem.Allocator,
    max_message_bytes: u32 = default_max_message_bytes,

    buf: std.ArrayList(u8) = .empty,
    /// Read cursor into `buf`; everything before it has been yielded already.
    pos: usize = 0,

    pub fn init(allocator: std.mem.Allocator) Decoder {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Decoder) void {
        self.buf.deinit(self.allocator);
    }

    pub const Message = struct {
        payload: []const u8,
        compressed: bool,
    };

    /// Feeds a chunk of stream bytes. Invalidates any payload previously
    /// returned by `next`.
    pub fn push(self: *Decoder, chunk: []const u8) !void {
        self.compact();
        try self.buf.appendSlice(self.allocator, chunk);
    }

    /// Returns the next complete message, or null if more bytes are needed.
    ///
    /// The payload borrows the decoder's buffer and stays valid until the next
    /// `push`. Callers that retain it beyond that must copy.
    pub fn next(self: *Decoder) !?Message {
        const available = self.buf.items[self.pos..];
        if (available.len < header_len) return null;

        const hdr = try Header.decode(available[0..header_len]);
        if (hdr.length > self.max_message_bytes) return error.MessageTooLarge;

        const total = header_len + @as(usize, hdr.length);
        // Leave the cursor untouched until the whole message has landed, so a
        // partial message is simply retried on the next push.
        if (available.len < total) return null;

        self.pos += total;
        return .{
            .payload = available[header_len..total],
            .compressed = hdr.compressed,
        };
    }

    /// True once every byte pushed so far has been yielded as a message.
    /// A stream that ends while this is false was truncated mid-message.
    pub fn isDrained(self: *const Decoder) bool {
        return self.pos == self.buf.items.len;
    }

    fn compact(self: *Decoder) void {
        if (self.pos == 0) return;
        const remaining = self.buf.items.len - self.pos;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buf.items[0..remaining], self.buf.items[self.pos..]);
        }
        self.buf.shrinkRetainingCapacity(remaining);
        self.pos = 0;
    }
};

test "header encodes to the documented 5-byte layout" {
    const hdr: Header = .{ .compressed = false, .length = 5 };
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0x00, 0x00, 0x05 }, &hdr.encode());
}

test "header length is big-endian" {
    const hdr: Header = .{ .compressed = true, .length = 0x0102_0304 };
    try std.testing.expectEqualSlices(u8, &.{ 0x01, 0x01, 0x02, 0x03, 0x04 }, &hdr.encode());

    const back = try Header.decode(&hdr.encode());
    try std.testing.expectEqual(@as(u32, 0x0102_0304), back.length);
    try std.testing.expect(back.compressed);
}

test "invalid compression flag is rejected" {
    try std.testing.expectError(
        error.InvalidCompressionFlag,
        Header.decode(&.{ 0x02, 0, 0, 0, 1 }),
    );
}

test "writeMessage emits header followed by payload" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeMessage(&w, "hello");
    try std.testing.expectEqualSlices(
        u8,
        &.{ 0x00, 0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o' },
        w.buffered(),
    );
}

test "writeMessage handles a zero-length message" {
    var buf: [16]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeMessage(&w, "");
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0, 0 }, w.buffered());
}

test "decoder yields a message delivered in one chunk" {
    var d = Decoder.init(std.testing.allocator);
    defer d.deinit();

    try d.push(&.{ 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o' });
    const msg = (try d.next()).?;
    try std.testing.expectEqualStrings("hello", msg.payload);
    try std.testing.expect(!msg.compressed);
    try std.testing.expectEqual(@as(?Decoder.Message, null), try d.next());
    try std.testing.expect(d.isDrained());
}

test "decoder reassembles a message split across DATA frames" {
    var d = Decoder.init(std.testing.allocator);
    defer d.deinit();

    // Header split mid-way, then payload split again.
    try d.push(&.{ 0, 0, 0 });
    try std.testing.expectEqual(@as(?Decoder.Message, null), try d.next());
    try d.push(&.{ 0, 5, 'h', 'e' });
    try std.testing.expectEqual(@as(?Decoder.Message, null), try d.next());
    try d.push("llo");

    const msg = (try d.next()).?;
    try std.testing.expectEqualStrings("hello", msg.payload);
}

test "decoder yields several messages from a single chunk" {
    var d = Decoder.init(std.testing.allocator);
    defer d.deinit();

    try d.push(&.{
        0, 0, 0, 0, 2, 'h', 'i',
        0, 0, 0, 0, 3, 'b', 'y', 'e',
        0, 0, 0, 0, 0,
    });

    try std.testing.expectEqualStrings("hi", (try d.next()).?.payload);
    try std.testing.expectEqualStrings("bye", (try d.next()).?.payload);
    try std.testing.expectEqualStrings("", (try d.next()).?.payload);
    try std.testing.expectEqual(@as(?Decoder.Message, null), try d.next());
    try std.testing.expect(d.isDrained());
}

test "decoder survives byte-at-a-time delivery" {
    var d = Decoder.init(std.testing.allocator);
    defer d.deinit();

    const wire = [_]u8{ 0, 0, 0, 0, 4, 'z', 'g', 'r', 'p' };
    var got: ?Decoder.Message = null;
    for (wire) |b| {
        try d.push(&.{b});
        if (try d.next()) |m| got = m;
    }
    try std.testing.expectEqualStrings("zgrp", got.?.payload);
}

test "decoder rejects an over-long message before allocating it" {
    var d = Decoder.init(std.testing.allocator);
    defer d.deinit();
    d.max_message_bytes = 8;

    // Advertises 64KB but supplies only the header.
    try d.push(&.{ 0, 0, 1, 0, 0 });
    try std.testing.expectError(error.MessageTooLarge, d.next());
}

test "decoder reports truncation when a stream ends mid-message" {
    var d = Decoder.init(std.testing.allocator);
    defer d.deinit();

    try d.push(&.{ 0, 0, 0, 0, 5, 'h', 'e' });
    try std.testing.expectEqual(@as(?Decoder.Message, null), try d.next());
    try std.testing.expect(!d.isDrained());
}

test "round-trip: writeMessage output decodes back to the same payloads" {
    const alloc = std.testing.allocator;
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(alloc);

    const payloads = [_][]const u8{ "", "a", "quick brown fox", "\x00\xff\x00" };
    var buf: [256]u8 = undefined;
    for (payloads) |p| {
        var w = std.Io.Writer.fixed(&buf);
        try writeMessage(&w, p);
        try wire.appendSlice(alloc, w.buffered());
    }

    var d = Decoder.init(alloc);
    defer d.deinit();
    try d.push(wire.items);
    for (payloads) |expected| {
        try std.testing.expectEqualStrings(expected, (try d.next()).?.payload);
    }
    try std.testing.expect(d.isDrained());
}
