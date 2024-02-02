const std = @import("std");
const types = @import("types.zig");
const testing = std.testing;
const Error = types.Error;

fn DeserializeValue(comptime T: type) type {
    return struct {
        result: T,
        rest: []const u8,
    };
}

fn serialize_int(comptime T: type, buf: *std.ArrayList(u8), n: T) Error!void {
    var slice: [@sizeOf(T)]u8 = undefined;
    std.mem.writeIntSliceBig(T, &slice, n);
    try buf.appendSlice(&slice);
}

fn deserialize_int(comptime T: type, buf: []const u8) Error!DeserializeValue(T) {
    if (buf.len < @sizeOf(T)) {
        return Error.DeserializeError;
    }
    const result = std.mem.readIntSlice(T, buf, std.builtin.Endian.Big);
    return DeserializeValue(T){ .result = result, .rest = buf[@sizeOf(T)..] };
}

fn serialize_string(buf: *std.ArrayList(u8), s: []const u8) Error!void {
    try serialize_int(usize, buf, s.len);
    try buf.appendSlice(s);
}

fn deserialize_string(buf: []const u8) Error!DeserializeValue([]const u8) {
    const int_value = try deserialize_int(usize, buf);
    const len = int_value.result;
    const rest = int_value.rest;
    if (rest.len < len) {
        return Error.DeserializeError;
    }

    return DeserializeValue([]const u8){ .result = rest[0..len], .rest = rest[len..] };
}

fn serialize_column(buf: *std.ArrayList(u8), column: types.ColumnMetadata) Error!void {
    try serialize_int(u8, buf, @intFromEnum(column.ty));
    try serialize_string(buf, column.name);
}

fn deserialize_column(buf: []const u8, index: u64) Error!DeserializeValue(types.ColumnMetadata) {
    const ty_value = try deserialize_int(u8, buf);
    const ty = @as(types.Type, @enumFromInt(ty_value.result));
    var rest = ty_value.rest;
    const name_value = try deserialize_string(rest);
    const name = name_value.result;
    rest = name_value.rest;
    return DeserializeValue(types.ColumnMetadata){ .result = types.ColumnMetadata{ .name = name, .ty = ty, .id = index }, .rest = rest };
}

fn serialize_columns(buf: *std.ArrayList(u8), columns: []const types.ColumnMetadata) Error!void {
    try serialize_int(usize, buf, columns.len);
    for (columns) |column| {
        try serialize_column(buf, column);
    }
}

pub fn serialize_table_metadata(metadata: types.TableMetadata, allocator: std.mem.Allocator) Error!std.ArrayList(u8) {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    try serialize_string(&buf, metadata.name);
    try serialize_int(usize, &buf, metadata.last_insert_id);
    try serialize_columns(&buf, metadata.columns.items);
    return buf;
}

pub fn deserialize_table_metadata(buf: []const u8, allocator: std.mem.Allocator) Error!types.TableMetadata {
    const name_value = try deserialize_string(buf);
    const name = name_value.result;
    var rest = name_value.rest;
    const last_insert_id_value = try deserialize_int(usize, rest);
    const last_insert_id = last_insert_id_value.result;
    rest = last_insert_id_value.rest;

    const column_count_value = try deserialize_int(usize, rest);
    const column_count = column_count_value.result;
    rest = column_count_value.rest;
    var table = types.TableMetadata.init(allocator, name);
    errdefer table.deinit();

    table.set_last_id(last_insert_id);

    for (0..column_count) |i| {
        const column_value = try deserialize_column(rest, i);
        rest = column_value.rest;
        try table.add_column(column_value.result.name, column_value.result.ty);
    }

    return table;
}

pub fn serialize_value(value: types.Value, allocator: std.mem.Allocator) Error!std.ArrayList(u8) {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    switch (value) {
        types.Value.Int => |i| {
            try serialize_int(u8, &buf, 0);
            try serialize_int(i64, &buf, i);
        },
        types.Value.Text => |s| {
            try serialize_int(u8, &buf, 1);
            try serialize_string(&buf, s);
        },
    }
    return buf;
}

pub fn deserialize_value(buf: []const u8) Error!types.Value {
    const ty_value = try deserialize_int(u8, buf);
    const ty = ty_value.result;
    const rest = ty_value.rest;
    switch (ty) {
        0 => {
            const int_value = try deserialize_int(i64, rest);
            return types.Value{ .Int = int_value.result };
        },
        1 => {
            const text_value = try deserialize_string(rest);
            return types.Value{ .Text = text_value.result };
        },
        else => return Error.DeserializeError,
    }
}

test "serialize table metadata" {
    var metadata = types.TableMetadata.init(std.testing.allocator, "foo");
    defer metadata.deinit();

    try metadata.add_column("bar", types.Type.Int);
    try metadata.add_column("baz", types.Type.Text);

    const expected = "\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x03bar\x01\x00\x00\x00\x00\x00\x00\x00\x03baz";
    const result = try serialize_table_metadata(metadata, std.testing.allocator);
    defer result.deinit();
    try testing.expectEqualSlices(u8, expected, result.items);
}

test "deserialize table metadata" {
    const input = "\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x03bar\x01\x00\x00\x00\x00\x00\x00\x00\x03baz";

    var expected = types.TableMetadata.init(std.testing.allocator, "foo");
    defer expected.deinit();

    try expected.add_column("bar", types.Type.Int);
    try expected.add_column("baz", types.Type.Text);

    const result = try deserialize_table_metadata(input, std.testing.allocator);
    defer result.deinit();

    try testing.expectEqualDeep(expected, result);
}

test "roundtrip serde" {
    var expected = types.TableMetadata.init(std.testing.allocator, "foo");
    defer expected.deinit();

    try expected.add_column("bar", types.Type.Int);
    try expected.add_column("baz", types.Type.Text);

    const serialized = try serialize_table_metadata(expected, std.testing.allocator);
    defer serialized.deinit();

    const result = try deserialize_table_metadata(serialized.items, std.testing.allocator);
    defer result.deinit();

    try testing.expectEqualDeep(expected, result);
}

test "serialize value" {
    const expected = "\x00\x00\x00\x00\x00\x00\x00\x00\x01";
    const result = try serialize_value(types.Value{ .Int = 1 }, std.testing.allocator);
    defer result.deinit();
    try testing.expectEqualSlices(u8, expected, result.items);
}
