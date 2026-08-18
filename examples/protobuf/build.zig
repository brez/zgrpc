const std = @import("std");
const protobuf = @import("protobuf");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zgrpc_dep = b.dependency("zgrpc", .{ .target = target, .optimize = optimize });
    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "protobuf_echo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zgrpc", .module = zgrpc_dep.module("zgrpc") },
                .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
            },
        }),
    });
    b.installArtifact(exe);

    const run_step = b.step("run", "Run the protobuf example");
    run_step.dependOn(&b.addRunArtifact(exe).step);

    // Regenerates src/helloworld.pb.zig, which is committed so that building
    // this example needs neither protoc nor a network. Run it after editing
    // the .proto.
    const gen_step = b.step("gen-proto", "Regenerate Zig types from proto/");
    gen_step.dependOn(&protobuf.RunProtocStep.create(protobuf_dep.builder, target, .{
        .destination_directory = b.path("src"),
        .source_files = &.{b.path("proto/helloworld.proto")},
        // Must contain the .proto files: protoc rejects any path it cannot see
        // below one of these, which the snippet in zig-protobuf's own README
        // gets wrong for protos in a subdirectory.
        .include_directories = &.{b.path("proto")},
    }).step);
}
