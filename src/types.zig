const std = @import("std");

pub const String = []const u8;

pub const Type = enum(u8) {
    Int = 0,
    Text = 1,
};

pub const Value = union(enum) {
    Int: i64,
    Text: String,

    pub fn eql(self: *const Value, other: *const Value) bool {
        switch (self.*) {
            .Int => {
                switch (other.*) {
                    .Int => return self.Int == other.Int,
                    .Text => return false,
                }
            },
            .Text => {
                switch (other.*) {
                    .Int => return false,
                    .Text => return std.mem.eql(u8, self.Text, other.Text),
                }
            },
        }
    }

    pub fn lt(self: *const Value, other: *const Value) bool {
        switch (self.*) {
            .Int => {
                switch (other.*) {
                    .Int => return self.Int < other.Int,
                    .Text => return false,
                }
            },
            .Text => {
                switch (other.*) {
                    .Int => return false,
                    .Text => return std.mem.lessThan(u8, self.Text, other.Text),
                }
            },
        }
    }

    pub fn gt(self: *const Value, other: *const Value) bool {
        switch (self.*) {
            .Int => {
                switch (other.*) {
                    .Int => return self.Int > other.Int,
                    .Text => return false,
                }
            },
            .Text => {
                switch (other.*) {
                    .Int => return false,
                    .Text => return !std.mem.lessThan(u8, self.Text, other.Text) and !std.mem.eql(u8, self.Text, other.Text),
                }
            },
        }
    }
};

pub const ColumnMetadata = struct {
    name: String,
    ty: Type,
    id: usize,
};

pub const Error = error{ FailedToOpen, FailedToCreateTable, BadType, InternalError, WrongNumberOfColumns, ColumnNotFound, DeserializeError, OutOfMemory, ParserError };

pub const Cell = struct {
    value: Value,
    bytes: []const u8,
};

pub const Row = struct {
    id: usize,
    cells: std.ArrayList(Cell),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, id: usize) Row {
        var cells = std.ArrayList(Cell).init(allocator);
        return Row{
            .id = id,
            .cells = cells,
            .allocator = allocator,
        };
    }

    pub fn add_value(self: *Row, value: Value, bytes: []const u8) std.mem.Allocator.Error!void {
        try self.cells.append(Cell{ .value = value, .bytes = bytes });
    }

    pub fn deinit(self: Row) void {
        for (self.cells.items) |cell| {
            self.allocator.free(cell.bytes);
        }
        self.cells.deinit();
    }
};

pub const Result = struct {
    rows: std.ArrayList(Row),

    pub fn init(allocator: std.mem.Allocator) Result {
        var rows = std.ArrayList(Row).init(allocator);
        return Result{
            .rows = rows,
        };
    }

    pub fn add_row(self: *Result, row: Row) std.mem.Allocator.Error!void {
        try self.rows.append(row);
    }

    pub fn deinit(self: Result) void {
        for (self.rows.items) |row| {
            row.deinit();
        }
        self.rows.deinit();
    }
};

pub const TableMetadata = struct {
    name: String,
    columns: std.ArrayList(ColumnMetadata),
    last_insert_id: usize,

    pub fn init(allocator: std.mem.Allocator, name: String) TableMetadata {
        return TableMetadata{
            .name = name,
            .columns = std.ArrayList(ColumnMetadata).init(allocator),
            .last_insert_id = 0,
        };
    }

    pub fn initCapacity(allocator: std.mem.Allocator, name: String, capacity: usize) std.mem.Allocator.Error!TableMetadata {
        return TableMetadata{
            .name = name,
            .columns = try std.ArrayList(ColumnMetadata).initCapacity(allocator, capacity),
            .last_insert_id = 0,
        };
    }

    pub fn add_column(self: *TableMetadata, name: String, ty: Type) std.mem.Allocator.Error!void {
        const len = self.columns.items.len;
        try self.columns.append(ColumnMetadata{
            .name = name,
            .ty = ty,
            .id = len,
        });
    }

    pub fn get_next_id(self: *TableMetadata) usize {
        self.last_insert_id += 1;
        return self.last_insert_id;
    }

    pub fn set_last_id(self: *TableMetadata, id: usize) void {
        self.last_insert_id = id;
    }

    pub fn get_columns(self: *TableMetadata, allocator: std.mem.Allocator, names: std.ArrayList(String)) Error!std.ArrayList(ColumnMetadata) {
        if (names.items.len != self.columns.items.len) {
            return Error.WrongNumberOfColumns;
        }
        var columns = std.ArrayList(ColumnMetadata).init(allocator);
        errdefer columns.deinit();
        for (names.items) |name| {
            var found = false;
            for (self.columns.items) |column| {
                if (std.mem.eql(u8, column.name, name)) {
                    found = true;
                    try columns.append(column);
                    break;
                }
            }
            if (!found) {
                return Error.ColumnNotFound;
            }
        }
        return columns;
    }

    pub fn deinit(self: TableMetadata) void {
        self.columns.deinit();
    }
};
