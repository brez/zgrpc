const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });
    const http2_dep = b.dependency("http2", .{ .target = target, .optimize = optimize });

    const mod = b.addModule("zgrpc", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
            .{ .name = "http2", .module = http2_dep.module("http2") },
        },
    });

    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    // Integration tests open real sockets on loopback, so they live in their
    // own binary and can be run separately from the pure unit tests.
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
                .{ .name = "http2", .module = http2_dep.module("http2") },
            },
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const echo_server = b.addExecutable(.{
        .name = "echo_server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/echo_server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zgrpc", .module = mod }},
        }),
    });
    b.installArtifact(echo_server);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/bench.zig"),
            .target = target,
            // Latency numbers from a Debug build are meaningless.
            .optimize = .ReleaseFast,
            .imports = &.{.{ .name = "zgrpc", .module = mod }},
        }),
    });
    // Deliberately not part of the default install: a ReleaseFast build of the
    // benchmark costs minutes and nothing else needs it.
    const bench_install = b.addInstallArtifact(bench, .{});

    const bench_step = b.step("bench", "Run the loopback latency benchmark");
    bench_step.dependOn(&bench_install.step);
    bench_step.dependOn(&b.addRunArtifact(bench).step);

    // Compiling the benchmark without running it: CI can catch bit-rot in it
    // without depending on timings from a noisy shared runner.
    const bench_build_step = b.step("bench-build", "Compile the benchmark without running it");
    bench_build_step.dependOn(&bench_install.step);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    const unit_step = b.step("test-unit", "Run unit tests only");
    unit_step.dependOn(&run_mod_tests.step);

    const integration_step = b.step("test-integration", "Run integration tests only");
    integration_step.dependOn(&run_integration_tests.step);
}
