const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zgrpc = b.dependency("zgrpc", .{ .target = target, .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "consumer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zgrpc", .module = zgrpc.module("zgrpc") }},
        }),
    });
    b.installArtifact(exe);

    const run_step = b.step("run", "Run the consumer smoke test");
    run_step.dependOn(&b.addRunArtifact(exe).step);
}
