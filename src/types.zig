const std = @import("std");

pub const String = []const u8;

pub const Type = enum(u8) {
    Int = 0,
    Text = 1,
};

pub const ColumnMetadata = struct {
    name: String,
    ty: Type,
};

pub const TableMetadata = struct {
    name: String,
    columns: []const ColumnMetadata,
    allocator: ?std.mem.Allocator,

    pub fn deinit(self: TableMetadata) void {
        if (self.allocator) |allocator| {
            allocator.free(self.columns);
        }
    }
};
