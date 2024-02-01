const std = @import("std");
const types = @import("types.zig");

pub const Column = struct { name: types.String, ty: types.Type };

pub const CreateStatement = struct {
    name: types.String,
    columns: std.ArrayList(Column),

    pub fn deinit(self: CreateStatement) void {
        self.columns.deinit();
    }
};

pub const Statement = union(enum) {
    Create: CreateStatement,

    pub fn deinit(self: Statement) void {
        switch (self) {
            .Create => |create| create.deinit(),
        }
    }
};
