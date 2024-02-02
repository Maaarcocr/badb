const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.

const LinkedLibs = struct {
    llz4: bool,
    snappy: bool,
    zlib: bool,
    bzip2: bool,
};

fn readMakeConfig() LinkedLibs {
    const make_config_file = std.fs.cwd().openFile("rocksdb/make_config.mk", .{}) catch unreachable;
    defer make_config_file.close();

    const read = make_config_file.readToEndAlloc(std.heap.c_allocator, std.math.maxInt(usize)) catch unreachable;
    defer std.heap.c_allocator.free(read);

    var lines = std.mem.splitAny(u8, read, "\n");
    var llz4 = false;
    var snappy = false;
    var zlib = false;
    var bzip2 = false;
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "PLATFORM_LDFLAGS=")) {
            llz4 = std.mem.containsAtLeast(u8, line, 1, "-llz4");
            snappy = std.mem.containsAtLeast(u8, line, 1, "-lsnappy");
            zlib = std.mem.containsAtLeast(u8, line, 1, "-lz");
            bzip2 = std.mem.containsAtLeast(u8, line, 1, "-lbz2");
        }
    }

    return LinkedLibs{
        .llz4 = llz4,
        .snappy = snappy,
        .zlib = zlib,
        .bzip2 = bzip2,
    };
}

fn configureCompilation(c: *std.build.Step.Compile, lib_file: std.build.LazyPath) void {
    c.linkLibC();
    c.linkLibCpp();
    c.addIncludePath(std.build.LazyPath{ .path = "rocksdb/include" });
    c.addObjectFile(lib_file);
    // c.linkSystemLibrary2("lz4", .{ .weak = true, .needed = false });
    // c.linkSystemLibraryWeak("snappy");
    c.linkSystemLibraryWeak("z");
    c.linkSystemLibraryWeak("bz2");
}

pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // const build_rocksdb = b.addSystemCommand(&[_][]const u8{ "make", "-j", "8", "-C", "rocksdb", "DESTDIR=" });
    // var lib_dir = build_rocksdb.addOutputFileArg("rocksdb");
    // build_rocksdb.addArg("install-static");
    var lib_file = std.build.LazyPath{ .path = "rocksdb/librocksdb.a" };

    const lib = b.addStaticLibrary(.{
        .name = "badb",
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    configureCompilation(lib, lib_file);

    // This declares intent for the library to be installed into the standard
    // location when the user invokes the "install" step (the default step when
    // running `zig build`).
    const install_step = b.addInstallArtifact(lib, .{});
    // install_step.step.dependOn(&build_rocksdb.step);
    b.getInstallStep().dependOn(&install_step.step);

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });

    configureCompilation(main_tests, lib_file);

    const run_main_tests = b.addRunArtifact(main_tests);
    // run_main_tests.step.dependOn(&build_rocksdb.step);

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build test`
    // This will evaluate the `test` step rather than the default, which is "install".
    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);
}
