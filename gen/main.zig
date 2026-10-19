//! protoc-gen-zgrpc: turns the `service` blocks of a .proto into zgrpc stubs.
//!
//! Runs beside zig-protobuf's own plugin, not instead of it. That one writes
//! the message types (`<package>.pb.zig`); this one writes `<package>.zgrpc.zig`
//! next to it, importing those types, with for each service
//!
//!   - the method paths, so none is typed by hand and none can be mistyped;
//!   - `Methods(Impl).all`, the `[]zgrpc.Method` a server takes, built from an
//!     `Impl` namespace of typed functions;
//!   - a `Client` with one typed function per method.
//!
//! All the decoding, encoding and error mapping is `zgrpc.typed`, which is
//! tested without protoc. What is emitted here is only the names.
//!
//! Output is per package, mirroring zig-protobuf, because the files it imports
//! are: `a.b` becomes `a/b.zgrpc.zig`, importing `b.pb.zig`. Several .proto
//! files in one package share one output file.
//!
//! Unary and server-streaming methods only, which is what zgrpc speaks. A
//! client- or bidirectionally-streaming method is an error rather than a
//! silent omission: a service with a method missing from its stub is a
//! worse surprise than one that does not generate.

const std = @import("std");
const plugin = @import("google/protobuf/compiler.pb.zig");
const descriptor = @import("google/protobuf.pb.zig");

const Allocator = std.mem.Allocator;

/// Writing to an in-memory buffer fails only when the allocation under it
/// does, but the writer interface reports that as `WriteFailed`.
const WriteError = Allocator.Error || error{WriteFailed};

pub fn main() !void {
    var arena_state: std.heap.ArenaAllocator = .init(std.heap.smp_allocator);
    const arena = arena_state.allocator();

    var threaded: std.Io.Threaded = .init(arena, .{ .environ = .empty });
    const io = threaded.io();

    var stdin_buf: [4096]u8 = undefined;
    var stdin = std.Io.File.stdin().reader(io, &stdin_buf);
    const request: plugin.CodeGeneratorRequest = try .decode(&stdin.interface, arena);

    const response = try generate(arena, request);

    var stdout_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buf);
    try response.encode(&stdout.interface, arena);
    try stdout.interface.flush();
}

/// The pure part: a request in, a response out, no I/O. Failures the .proto
/// causes go in `response.error`, which protoc shows the user; only running out
/// of memory is an error here.
pub fn generate(arena: Allocator, request: plugin.CodeGeneratorRequest) Allocator.Error!plugin.CodeGeneratorResponse {
    var response: plugin.CodeGeneratorResponse = .{};
    response.supported_features = @intFromEnum(plugin.CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL);

    // Every package protoc knows of, so a type name can be split into the
    // package it lives in and the path inside it.
    var known: std.StringHashMapUnmanaged(void) = .empty;
    for (request.proto_file.items) |file| {
        if (file.package) |package| try known.put(arena, package, {});
    }

    // Packages with services, in the order protoc listed the files.
    var packages: std.ArrayList(*Package) = .empty;
    for (request.file_to_generate.items) |name| {
        const file = for (request.proto_file.items) |*candidate| {
            if (std.mem.eql(u8, candidate.name orelse "", name)) break candidate;
        } else continue;
        if (file.service.items.len == 0) continue;

        const package_name = file.package orelse {
            response.@"error" = try std.fmt.allocPrint(
                arena,
                "{s}: a package is required, because zig-protobuf writes one file per package and the stubs import it",
                .{name},
            );
            return response;
        };

        const package = for (packages.items) |existing| {
            if (std.mem.eql(u8, existing.name, package_name)) break existing;
        } else blk: {
            const created = try arena.create(Package);
            created.* = .init(arena, package_name, &known);
            try packages.append(arena, created);
            break :blk created;
        };

        for (file.service.items) |service| {
            package.emitService(service) catch |err| switch (err) {
                error.OutOfMemory, error.WriteFailed => return error.OutOfMemory,
                error.Unsupported => {
                    response.@"error" = package.problem;
                    return response;
                },
            };
        }
    }

    for (packages.items) |package| {
        var file: plugin.CodeGeneratorResponse.File = .{};
        file.name = try std.fmt.allocPrint(arena, "{s}.zgrpc.zig", .{try packagePath(arena, package.name)});
        file.content = package.finish() catch return error.OutOfMemory;
        try response.file.append(arena, file);
    }
    return response;
}

const Package = struct {
    arena: Allocator,
    name: []const u8,
    known: *const std.StringHashMapUnmanaged(void),
    /// Other packages whose message types the services use, in first-use order.
    imports: std.ArrayList([]const u8) = .empty,
    body: std.Io.Writer.Allocating,
    /// Why `emitService` said `Unsupported`.
    problem: []const u8 = "",

    const Error = WriteError || error{Unsupported};

    fn init(arena: Allocator, name: []const u8, known: *const std.StringHashMapUnmanaged(void)) Package {
        return .{ .arena = arena, .name = name, .known = known, .body = .init(arena) };
    }

    /// The message type a `.pkg.Message` name from a descriptor is, as Zig
    /// source: `pb.Message` in this package, `pkg_other.Message` in another.
    fn typeRef(self: *Package, proto_name: []const u8) Error![]const u8 {
        const bare = if (std.mem.startsWith(u8, proto_name, ".")) proto_name[1..] else proto_name;

        // The longest known package that prefixes the name, since a package
        // `a` and a message `a.b.C` could also be package `a.b`, message `C`.
        var best: ?[]const u8 = null;
        var it = self.known.keyIterator();
        while (it.next()) |candidate| {
            const package = candidate.*;
            if (bare.len > package.len and bare[package.len] == '.' and std.mem.startsWith(u8, bare, package)) {
                if (best == null or package.len > best.?.len) best = package;
            }
        }
        const package = best orelse {
            self.problem = try std.fmt.allocPrint(self.arena, "cannot place message type {s} in any package protoc was given", .{proto_name});
            return error.Unsupported;
        };
        const rest = bare[package.len + 1 ..];

        if (std.mem.eql(u8, package, self.name)) {
            return std.fmt.allocPrint(self.arena, "pb.{s}", .{rest});
        }
        for (self.imports.items) |seen| {
            if (std.mem.eql(u8, seen, package)) break;
        } else try self.imports.append(self.arena, package);
        return std.fmt.allocPrint(self.arena, "{s}.{s}", .{ try importName(self.arena, package), rest });
    }

    fn emitService(self: *Package, service: descriptor.ServiceDescriptorProto) Error!void {
        const service_name = service.name.?;
        const w = &self.body.writer;

        // Resolve every type first: a failure should leave nothing half-written.
        const Resolved = struct { method: descriptor.MethodDescriptorProto, input: []const u8, output: []const u8, camel: []const u8 };
        var methods: std.ArrayList(Resolved) = .empty;
        for (service.method.items) |method| {
            const method_name = method.name.?;
            const client_streaming = method.client_streaming orelse false;
            const server_streaming = method.server_streaming orelse false;
            if (client_streaming) {
                self.problem = try std.fmt.allocPrint(
                    self.arena,
                    "{s}.{s} is {s}, and zgrpc supports unary and server-streaming methods only",
                    .{ service_name, method_name, if (server_streaming) "bidirectionally streaming" else "client-streaming" },
                );
                return error.Unsupported;
            }
            try methods.append(self.arena, .{
                .method = method,
                .input = try self.typeRef(method.input_type.?),
                .output = try self.typeRef(method.output_type.?),
                .camel = try lowerCamel(self.arena, method_name),
            });
        }

        try w.print(
            \\
            \\pub const {f} = struct {{
            \\    pub const full_name = "{s}.{s}";
            \\
            \\    /// Method paths, as they go on the wire.
            \\    pub const path = struct {{
            \\
        , .{ std.zig.fmtId(service_name), self.name, service_name });
        for (methods.items) |m| {
            try w.print("        pub const {f} = \"/{s}.{s}/{s}\";\n", .{ std.zig.fmtId(m.method.name.?), self.name, service_name, m.method.name.? });
        }
        try w.writeAll("    };\n");

        // Streaming handlers receive a sender, so the signature differs.
        try w.print(
            \\
            \\    /// The server side. `Impl` is a namespace with one function per method:
            \\    ///
            \\
        , .{});
        for (methods.items) |m| {
            if (m.method.server_streaming orelse false) {
                try w.print("    ///     pub fn {s}(ctx: *zgrpc.CallContext, request: {s}, sender: *zgrpc.typed.Sender({s})) anyerror!void\n", .{ m.camel, m.input, m.output });
            } else {
                try w.print("    ///     pub fn {s}(ctx: *zgrpc.CallContext, request: {s}) anyerror!{s}\n", .{ m.camel, m.input, m.output });
            }
        }
        try w.print(
            \\    ///
            \\    /// `Methods(Impl).all` is what `Server.listen` takes. State reaches the
            \\    /// functions through `ctx.userData(T)`; join several services with `++`.
            \\    pub fn Methods(comptime Impl: type) type {{
            \\        comptime {{
            \\
        , .{});
        for (methods.items) |m| {
            try w.print(
                "            if (!@hasDecl(Impl, \"{s}\")) @compileError(\"{s} implementation is missing `{s}`\");\n",
                .{ m.camel, service_name, m.camel },
            );
        }
        try w.writeAll(
            \\        }
            \\        return struct {
            \\            pub const all = [_]zgrpc.Method{
            \\
        );
        for (methods.items) |m| {
            const streaming = m.method.server_streaming orelse false;
            try w.print(
                "                zgrpc.Method.{s}(path.{f}, zgrpc.typed.{s}({s}, {s}, Impl.{f})),\n",
                .{
                    if (streaming) "serverStream" else "unary",
                    std.zig.fmtId(m.method.name.?),
                    if (streaming) "serverStreamHandler" else "unaryHandler",
                    m.input,
                    m.output,
                    std.zig.fmtId(m.camel),
                },
            );
        }
        try w.writeAll(
            \\            };
            \\        };
            \\    }
            \\
            \\    /// The client side, over a `zgrpc.Pool`.
            \\    pub const Client = struct {
            \\        pool: *zgrpc.Pool,
            \\
            \\        pub fn init(pool: *zgrpc.Pool) Client {
            \\            return .{ .pool = pool };
            \\        }
            \\
        );
        for (methods.items) |m| {
            const streaming = m.method.server_streaming orelse false;
            try w.print(
                \\
                \\        pub fn {f}(
                \\            self: Client,
                \\            allocator: std.mem.Allocator,
                \\            request: {s},
                \\            options: zgrpc.client.CallOptions,
                \\        ) !{s} {{
                \\            return zgrpc.typed.{s}({s}, {s}, self.pool, allocator, path.{f}, request, options);
                \\        }}
                \\
            , .{
                std.zig.fmtId(m.camel),
                m.input,
                if (streaming) try std.fmt.allocPrint(self.arena, "zgrpc.typed.Receiver({s})", .{m.output}) else try std.fmt.allocPrint(self.arena, "zgrpc.Reply({s})", .{m.output}),
                if (streaming) "callServerStream" else "call",
                m.input,
                m.output,
                std.zig.fmtId(m.method.name.?),
            });
        }
        try w.writeAll(
            \\    };
            \\};
            \\
        );
    }

    /// The finished file: header and imports, then the services.
    fn finish(self: *Package) WriteError![]const u8 {
        var out: std.Io.Writer.Allocating = .init(self.arena);
        const w = &out.writer;
        try w.print(
            \\// Code generated by protoc-gen-zgrpc. DO NOT EDIT.
            \\///! package {s}
            \\const std = @import("std");
            \\const zgrpc = @import("zgrpc");
            \\const pb = @import("{s}");
            \\
        , .{ self.name, try pbImport(self.arena, self.name, self.name) });
        for (self.imports.items) |other| {
            try w.print("const {s} = @import(\"{s}\");\n", .{
                try importName(self.arena, other),
                try pbImport(self.arena, self.name, other),
            });
        }
        try w.writeAll(self.body.written());
        return out.written();
    }
};

/// `Get` -> `get`, `GetUser` -> `getUser`: Zig functions are camelCase, and a
/// method name is the one thing a .proto spells in PascalCase.
fn lowerCamel(arena: Allocator, name: []const u8) Allocator.Error![]const u8 {
    const out = try arena.dupe(u8, name);
    if (out.len > 0) out[0] = std.ascii.toLower(out[0]);
    return out;
}

/// `a.b` -> `a/b`, the path zig-protobuf writes `a.b`'s file to (less the
/// extension).
fn packagePath(arena: Allocator, package: []const u8) Allocator.Error![]const u8 {
    const out = try arena.dupe(u8, package);
    std.mem.replaceScalar(u8, out, '.', '/');
    return out;
}

/// The constant a package's file is imported as: `pkg_a_b` for `a.b`.
fn importName(arena: Allocator, package: []const u8) Allocator.Error![]const u8 {
    const out = try std.fmt.allocPrint(arena, "pkg_{s}", .{package});
    std.mem.replaceScalar(u8, out, '.', '_');
    return out;
}

/// The `@import` path from `from`'s stub to `to`'s message file. Both sit
/// under one output directory, in a directory per dotted component.
fn pbImport(arena: Allocator, from: []const u8, to: []const u8) WriteError![]const u8 {
    var from_dirs: std.ArrayList([]const u8) = .empty;
    var to_dirs: std.ArrayList([]const u8) = .empty;
    var to_file: []const u8 = to;

    var it = std.mem.splitScalar(u8, from, '.');
    while (it.next()) |part| try from_dirs.append(arena, part);
    _ = from_dirs.pop(); // the last component names the file, not a directory

    var it_to = std.mem.splitScalar(u8, to, '.');
    while (it_to.next()) |part| {
        to_file = part;
        try to_dirs.append(arena, part);
    }
    _ = to_dirs.pop();

    var common: usize = 0;
    while (common < from_dirs.items.len and common < to_dirs.items.len and
        std.mem.eql(u8, from_dirs.items[common], to_dirs.items[common])) common += 1;

    var out: std.Io.Writer.Allocating = .init(arena);
    for (from_dirs.items[common..]) |_| try out.writer.writeAll("../");
    for (to_dirs.items[common..]) |dir| try out.writer.print("{s}/", .{dir});
    try out.writer.print("{s}.pb.zig", .{to_file});
    return out.written();
}

// -- tests -------------------------------------------------------------------

const testing = std.testing;

fn mkMethod(name: []const u8, input: []const u8, output: []const u8, server_streaming: bool, client_streaming: bool) descriptor.MethodDescriptorProto {
    return .{
        .name = name,
        .input_type = input,
        .output_type = output,
        .server_streaming = server_streaming,
        .client_streaming = client_streaming,
    };
}

fn mkFile(
    arena: Allocator,
    name: []const u8,
    package: []const u8,
    services: []const descriptor.ServiceDescriptorProto,
) !descriptor.FileDescriptorProto {
    var f: descriptor.FileDescriptorProto = .{ .name = name, .package = package };
    try f.service.appendSlice(arena, services);
    return f;
}

fn mkService(arena: Allocator, name: []const u8, methods: []const descriptor.MethodDescriptorProto) !descriptor.ServiceDescriptorProto {
    var s: descriptor.ServiceDescriptorProto = .{ .name = name };
    try s.method.appendSlice(arena, methods);
    return s;
}

fn mkRequest(arena: Allocator, generate_names: []const []const u8, files: []const descriptor.FileDescriptorProto) !plugin.CodeGeneratorRequest {
    var r: plugin.CodeGeneratorRequest = .{};
    try r.file_to_generate.appendSlice(arena, generate_names);
    try r.proto_file.appendSlice(arena, files);
    return r;
}

test "a unary and a streaming method become typed entries" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const greeter = try mkService(arena, "Greeter", &.{
        mkMethod("SayHello", ".helloworld.HelloRequest", ".helloworld.HelloReply", false, false),
        mkMethod("Watch", ".helloworld.HelloRequest", ".helloworld.HelloReply", true, false),
    });
    const response = try generate(arena, try mkRequest(arena, &.{"h.proto"}, &.{
        try mkFile(arena, "h.proto", "helloworld", &.{greeter}),
    }));

    try testing.expect(response.@"error" == null);
    try testing.expectEqual(@as(usize, 1), response.file.items.len);
    try testing.expectEqualStrings("helloworld.zgrpc.zig", response.file.items[0].name.?);

    const out = response.file.items[0].content.?;
    for ([_][]const u8{
        "const pb = @import(\"helloworld.pb.zig\");",
        "pub const SayHello = \"/helloworld.Greeter/SayHello\";",
        "pub const Watch = \"/helloworld.Greeter/Watch\";",
        "zgrpc.Method.unary(path.SayHello, zgrpc.typed.unaryHandler(pb.HelloRequest, pb.HelloReply, Impl.sayHello)),",
        "zgrpc.Method.serverStream(path.Watch, zgrpc.typed.serverStreamHandler(pb.HelloRequest, pb.HelloReply, Impl.watch)),",
        "implementation is missing `watch`",
        ") !zgrpc.Reply(pb.HelloReply) {",
        ") !zgrpc.typed.Receiver(pb.HelloReply) {",
        "sender: *zgrpc.typed.Sender(pb.HelloReply)) anyerror!void",
    }) |needle| {
        if (std.mem.indexOf(u8, out, needle) == null) {
            std.debug.print("missing {s}\n--- generated:\n{s}\n", .{ needle, out });
            return error.TestExpectedEqual;
        }
    }
}

test "types from another package are imported by relative path" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const svc = try mkService(arena, "Query", &.{
        mkMethod("Range", ".tsdb.v1.RangeRequest", ".tsdb.common.Point", true, false),
    });
    const response = try generate(arena, try mkRequest(arena, &.{"q.proto"}, &.{
        try mkFile(arena, "common.proto", "tsdb.common", &.{}),
        try mkFile(arena, "q.proto", "tsdb.v1", &.{svc}),
    }));

    try testing.expect(response.@"error" == null);
    try testing.expectEqualStrings("tsdb/v1.zgrpc.zig", response.file.items[0].name.?);
    const out = response.file.items[0].content.?;
    try testing.expect(std.mem.indexOf(u8, out, "const pb = @import(\"v1.pb.zig\");") != null);
    try testing.expect(std.mem.indexOf(u8, out, "const pkg_tsdb_common = @import(\"common.pb.zig\");") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zgrpc.typed.serverStreamHandler(pb.RangeRequest, pkg_tsdb_common.Point,") != null);
}

test "import paths climb out of a directory only as far as needed" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    try testing.expectEqualStrings("b.pb.zig", try pbImport(arena, "a.b", "a.b"));
    try testing.expectEqualStrings("c.pb.zig", try pbImport(arena, "a.b", "a.c"));
    try testing.expectEqualStrings("../x.pb.zig", try pbImport(arena, "a.b", "x"));
    try testing.expectEqualStrings("../x/y.pb.zig", try pbImport(arena, "a.b", "x.y"));
    try testing.expectEqualStrings("a/b.pb.zig", try pbImport(arena, "a", "a.b"));
    try testing.expectEqualStrings("a.pb.zig", try pbImport(arena, "a", "a"));
}

test "a method zgrpc cannot serve fails generation instead of vanishing" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const chat = try mkService(arena, "Chat", &.{
        mkMethod("Join", ".chat.Msg", ".chat.Msg", true, true),
    });
    const response = try generate(arena, try mkRequest(arena, &.{"c.proto"}, &.{
        try mkFile(arena, "c.proto", "chat", &.{chat}),
    }));

    try testing.expectEqual(@as(usize, 0), response.file.items.len);
    try testing.expectEqualStrings(
        "Chat.Join is bidirectionally streaming, and zgrpc supports unary and server-streaming methods only",
        response.@"error".?,
    );
}

test "a file without a package is refused, naming the file" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var f: descriptor.FileDescriptorProto = .{ .name = "bare.proto" };
    try f.service.append(arena, try mkService(arena, "S", &.{}));
    const response = try generate(arena, try mkRequest(arena, &.{"bare.proto"}, &.{f}));

    try testing.expect(std.mem.startsWith(u8, response.@"error".?, "bare.proto: a package is required"));
}

test "files with no services produce nothing" {
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const response = try generate(arena, try mkRequest(arena, &.{"m.proto"}, &.{
        try mkFile(arena, "m.proto", "msgs", &.{}),
    }));
    try testing.expect(response.@"error" == null);
    try testing.expectEqual(@as(usize, 0), response.file.items.len);
}
