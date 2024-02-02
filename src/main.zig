const std = @import("std");
const parser = @import("parser.zig");
const parser_types = @import("parser_types.zig");
const serde = @import("serde.zig");
const rocksdb = @import("rocksdb.zig");
const types = @import("types.zig");
const Error = types.Error;

const BadDB = struct {
    db: rocksdb.RocksDB,
    allocator: std.mem.Allocator,

    pub fn open(allocator: std.mem.Allocator, dir: []const u8) Error!BadDB {
        var db = rocksdb.RocksDB.open(allocator, dir);
        switch (db) {
            .val => |val| return .{ .db = val, .allocator = allocator },
            .err => return Error.FailedToOpen,
        }
    }

    pub fn run(self: *BadDB, sql: []const u8) Error!types.Result {
        var cursor = parser.Cursor.new(sql, self.allocator);
        var stmt = try parser.parse_statement(&cursor);
        defer stmt.deinit();

        switch (stmt) {
            .Create => |create_stmt| {
                var table = try types.TableMetadata.initCapacity(self.allocator, create_stmt.name, create_stmt.columns.items.len);
                defer table.deinit();

                for (create_stmt.columns.items) |column| {
                    try table.add_column(column.name, column.ty);
                }

                const table_bytes = try serde.serialize_table_metadata(table, self.allocator);
                defer table_bytes.deinit();

                const key = try std.fmt.allocPrint(self.allocator, "/tables/{s}", .{create_stmt.name});
                defer self.allocator.free(key);

                const err = self.db.set(key, table_bytes.items);
                if (err) |_| {
                    return Error.FailedToCreateTable;
                }
                return types.Result.init(self.allocator);
            },
            .Insert => |insert_stmt| {
                const table_key = try std.fmt.allocPrint(self.allocator, "/tables/{s}", .{insert_stmt.name});
                defer self.allocator.free(table_key);

                const table_bytes = self.db.get(table_key).val;
                defer self.allocator.free(table_bytes);

                var table = try serde.deserialize_table_metadata(table_bytes, self.allocator);
                defer table.deinit();

                const id = table.get_next_id();
                const new_table_bytes = try serde.serialize_table_metadata(table, self.allocator);
                defer new_table_bytes.deinit();

                var err = self.db.set(table_key, new_table_bytes.items);
                if (err) |_| {
                    return Error.InternalError;
                }

                const columns = if (insert_stmt.column_names) |names| blk: {
                    var columns = try table.get_columns(self.allocator, names);
                    const slice = try columns.toOwnedSlice();
                    break :blk slice;
                } else blk: {
                    break :blk try table.columns.toOwnedSlice();
                };
                defer self.allocator.free(columns);

                const values = insert_stmt.values.items;
                for (columns, values) |column, value| {
                    switch (value) {
                        .Int => {
                            if (column.ty != types.Type.Int) {
                                return Error.BadType;
                            }
                        },
                        .Text => {
                            if (column.ty != types.Type.Text) {
                                return Error.BadType;
                            }
                        },
                    }
                    const column_key = try std.fmt.allocPrint(self.allocator, "/tables/{s}/{d}/{s}", .{ insert_stmt.name, id, column.name });
                    defer self.allocator.free(column_key);

                    const value_bytes = try serde.serialize_value(value, self.allocator);
                    defer value_bytes.deinit();

                    err = self.db.set(column_key, value_bytes.items);
                    if (err) |_| {
                        return Error.InternalError;
                    }
                }
                return types.Result.init(self.allocator);
            },
            .Select => |select_stmt| {
                const table_key = try std.fmt.allocPrint(self.allocator, "/tables/{s}", .{select_stmt.name});
                defer self.allocator.free(table_key);

                const table_bytes = self.db.get(table_key).val;
                defer self.allocator.free(table_bytes);

                var table = try serde.deserialize_table_metadata(table_bytes, self.allocator);
                defer table.deinit();

                const columns = if (select_stmt.column_names) |names| blk: {
                    var columns = try table.get_columns(self.allocator, names);
                    const slice = try columns.toOwnedSlice();
                    break :blk slice;
                } else blk: {
                    break :blk try table.columns.toOwnedSlice();
                };
                defer self.allocator.free(columns);

                var result = types.Result.init(self.allocator);

                for (1..table.last_insert_id + 1) |id| {
                    if (select_stmt.where_clause) |where_clause| {
                        const lhf = try self.resolve_atom(where_clause.Comparison.lhs, select_stmt.name, id);
                        const rhf = try self.resolve_atom(where_clause.Comparison.rhs, select_stmt.name, id);
                        const is_true = switch (where_clause.Comparison.op) {
                            .Eq => lhf.eql(&rhf),
                            .Lt => lhf.lt(&rhf),
                            .Gt => lhf.gt(&rhf),
                        };
                        if (is_true) {
                            const row = try self.get_row(self.allocator, select_stmt.name, id, columns);
                            try result.add_row(row);
                        }
                    } else {
                        const row = try self.get_row(self.allocator, select_stmt.name, id, columns);
                        try result.add_row(row);
                    }
                }

                return result;
            },
        }
    }

    fn get_row(self: *BadDB, allocator: std.mem.Allocator, table_name: types.String, id: usize, columns: []types.ColumnMetadata) Error!types.Row {
        var row = types.Row.init(allocator, id);
        for (columns) |column| {
            const column_key = try std.fmt.allocPrint(allocator, "/tables/{s}/{d}/{s}", .{ table_name, id, column.name });
            defer allocator.free(column_key);

            const value_bytes = self.db.get(column_key).val;
            errdefer allocator.free(value_bytes);

            const value = try serde.deserialize_value(value_bytes);
            try row.add_value(value, value_bytes);
        }
        return row;
    }

    fn resolve_atom(self: *BadDB, atom: parser_types.Atom, table_name: types.String, column_id: usize) Error!types.Value {
        switch (atom) {
            .IntLiteral => |i| return types.Value{ .Int = i },
            .StringLiteral => |s| return types.Value{ .Text = s },
            .Identifier => |name| {
                const column_key = try std.fmt.allocPrint(self.allocator, "/tables/{s}/{d}/{s}", .{ table_name, column_id, name });
                defer self.allocator.free(column_key);

                const value_bytes = self.db.get(column_key).val;
                defer self.allocator.free(value_bytes);

                return try serde.deserialize_value(value_bytes);
            },
        }
    }

    pub fn dump(self: *BadDB) void {
        var iter = self.db.iter("/tables").val;
        while (iter.next()) |entry| {
            std.debug.print("__________________\n", .{});
            std.debug.print("key: {s}\n", .{entry.key});
            std.debug.print("value: {s}\n", .{entry.value});
            std.debug.print("__________________\n", .{});
        }
    }

    pub fn close(self: *BadDB) void {
        self.db.close();
    }
};

const testing = std.testing;

test "opening a db and create table" {
    var allocator = std.testing.allocator;
    const tmpDir = std.testing.tmpDir(.{});
    const tmpDirName = try tmpDir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpDirName);

    var db = try BadDB.open(allocator, tmpDirName);
    defer db.close();

    const sql = "CREATE TABLE users (id INT, name TEXT)";
    const result = try db.run(sql);
    defer result.deinit();

    const key = try std.fmt.allocPrint(allocator, "/tables/{s}", .{"users"});
    defer allocator.free(key);

    const table_bytes = db.db.get(key).val;
    defer allocator.free(table_bytes);

    const table = try serde.deserialize_table_metadata(table_bytes, allocator);
    defer table.deinit();

    try testing.expectEqualStrings(table.name, "users");
    try testing.expectEqual(table.columns.items.len, 2);
    try testing.expectEqualStrings(table.columns.items[0].name, "id");
    try testing.expectEqual(table.columns.items[0].ty, types.Type.Int);
    try testing.expectEqualStrings(table.columns.items[1].name, "name");
    try testing.expectEqual(table.columns.items[1].ty, types.Type.Text);
}

test "opening a db, create table and insert" {
    var allocator = std.testing.allocator;
    const tmpDir = std.testing.tmpDir(.{});
    const tmpDirName = try tmpDir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpDirName);

    var db = try BadDB.open(allocator, tmpDirName);
    defer db.close();

    const sql = "CREATE TABLE users (age INT, name TEXT)";
    const result = try db.run(sql);
    defer result.deinit();

    const insertSql = "INSERT INTO users (name, age) VALUES ('marco', 28)";
    const insertResult = try db.run(insertSql);
    defer insertResult.deinit();

    const firstValueKey = try std.fmt.allocPrint(allocator, "/tables/{s}/{d}/{s}", .{ "users", 1, "age" });
    defer allocator.free(firstValueKey);

    const firstValueBytes = db.db.get(firstValueKey).val;
    defer allocator.free(firstValueBytes);

    const firstValue = try serde.deserialize_value(firstValueBytes);
    try testing.expectEqualDeep(firstValue, types.Value{ .Int = 28 });

    const secondValueKey = try std.fmt.allocPrint(allocator, "/tables/{s}/{d}/{s}", .{ "users", 1, "name" });
    defer allocator.free(secondValueKey);

    const secondValueBytes = db.db.get(secondValueKey).val;
    defer allocator.free(secondValueBytes);

    const secondValue = try serde.deserialize_value(secondValueBytes);
    try testing.expectEqualDeep(secondValue, types.Value{ .Text = "marco" });
}

test "opening a db, create table and insert without names" {
    var allocator = std.testing.allocator;
    const tmpDir = std.testing.tmpDir(.{});
    const tmpDirName = try tmpDir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpDirName);

    var db = try BadDB.open(allocator, tmpDirName);
    defer db.close();

    const sql = "CREATE TABLE users (age INT, name TEXT)";
    const result = try db.run(sql);
    defer result.deinit();

    const insertSql = "INSERT INTO users VALUES (28, 'marco')";
    const insertResult = try db.run(insertSql);
    defer insertResult.deinit();

    const firstValueKey = try std.fmt.allocPrint(allocator, "/tables/{s}/{d}/{s}", .{ "users", 1, "age" });
    defer allocator.free(firstValueKey);

    const firstValueBytes = db.db.get(firstValueKey).val;
    defer allocator.free(firstValueBytes);

    const firstValue = try serde.deserialize_value(firstValueBytes);
    try testing.expectEqualDeep(firstValue, types.Value{ .Int = 28 });

    const secondValueKey = try std.fmt.allocPrint(allocator, "/tables/{s}/{d}/{s}", .{ "users", 1, "name" });
    defer allocator.free(secondValueKey);

    const secondValueBytes = db.db.get(secondValueKey).val;
    defer allocator.free(secondValueBytes);

    const secondValue = try serde.deserialize_value(secondValueBytes);
    try testing.expectEqualDeep(secondValue, types.Value{ .Text = "marco" });
}

test "opening a db, create table and insert and select" {
    var allocator = std.testing.allocator;
    const tmpDir = std.testing.tmpDir(.{});
    const tmpDirName = try tmpDir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpDirName);

    var db = try BadDB.open(allocator, tmpDirName);
    defer db.close();

    const sql = "CREATE TABLE users (age INT, name TEXT)";
    const result = try db.run(sql);
    defer result.deinit();

    const insertSql = "INSERT INTO users VALUES (28, 'marco')";
    const insertResult = try db.run(insertSql);
    defer insertResult.deinit();

    const selectSql = "SELECT * FROM users";
    const selectResult = try db.run(selectSql);
    defer selectResult.deinit();

    try testing.expectEqual(selectResult.rows.items.len, 1);
    try testing.expectEqual(selectResult.rows.items[0].cells.items.len, 2);
    try testing.expectEqualDeep(selectResult.rows.items[0].cells.items[0].value, types.Value{ .Int = 28 });
    try testing.expectEqualDeep(selectResult.rows.items[0].cells.items[1].value, types.Value{ .Text = "marco" });
}

test "opening a db, create table and insert and select with where" {
    var allocator = std.testing.allocator;
    const tmpDir = std.testing.tmpDir(.{});
    const tmpDirName = try tmpDir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpDirName);

    var db = try BadDB.open(allocator, tmpDirName);
    defer db.close();

    const sql = "CREATE TABLE users (age INT, name TEXT)";
    const result = try db.run(sql);
    defer result.deinit();

    const insertSql = "INSERT INTO users VALUES (28, 'marco')";
    const insertResult = try db.run(insertSql);
    defer insertResult.deinit();

    const insertSql2 = "INSERT INTO users VALUES (16, 'simone')";
    const insertResult2 = try db.run(insertSql2);
    defer insertResult2.deinit();

    const selectSql = "SELECT * FROM users WHERE age = 28";
    const selectResult = try db.run(selectSql);
    defer selectResult.deinit();

    try testing.expectEqual(selectResult.rows.items.len, 1);
    try testing.expectEqual(selectResult.rows.items[0].cells.items.len, 2);
    try testing.expectEqualDeep(selectResult.rows.items[0].cells.items[0].value, types.Value{ .Int = 28 });
    try testing.expectEqualDeep(selectResult.rows.items[0].cells.items[1].value, types.Value{ .Text = "marco" });
}
