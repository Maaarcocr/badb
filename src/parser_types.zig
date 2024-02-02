const std = @import("std");
const types = @import("types.zig");

pub const Column = struct { name: types.String, ty: types.Type };

pub const Atom = union(enum) {
    IntLiteral: i64,
    StringLiteral: types.String,
    Identifier: types.String,
};

pub const ComparisonOp = enum {
    Eq,
    Lt,
    Gt,
};

pub const Comparison = struct {
    lhs: Atom,
    rhs: Atom,
    op: ComparisonOp,
};

pub const Expression = union(enum) {
    Comparison: Comparison,
};

pub const CreateStatement = struct {
    name: types.String,
    columns: std.ArrayList(Column),

    pub fn deinit(self: CreateStatement) void {
        self.columns.deinit();
    }
};

pub const InsertStatement = struct {
    name: types.String,
    column_names: ?std.ArrayList(types.String),
    values: std.ArrayList(types.Value),

    pub fn deinit(self: InsertStatement) void {
        self.values.deinit();
        if (self.column_names) |column_names| column_names.deinit();
    }
};

pub const SelectStatement = struct {
    name: types.String,
    column_names: ?std.ArrayList(types.String),
    where_clause: ?Expression,

    pub fn deinit(self: SelectStatement) void {
        if (self.column_names) |columns| columns.deinit();
    }
};

pub const Statement = union(enum) {
    Create: CreateStatement,
    Insert: InsertStatement,
    Select: SelectStatement,

    pub fn deinit(self: Statement) void {
        switch (self) {
            .Create => |create| create.deinit(),
            .Insert => |insert| insert.deinit(),
            .Select => |select| select.deinit(),
        }
    }
};
