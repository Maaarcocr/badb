const std = @import("std");
const types = @import("types.zig");
const testing = std.testing;

const AllocError = std.mem.Allocator.Error;
const DeserializeError = error{ DeserializeError, OutOfMemory };

fn DeserializeValue(comptime T: type) type {
    return struct {
        result: T,
        rest: []const u8,
    };
}

fn serialize_int(comptime T: type, buf: *std.ArrayList(u8), n: T) AllocError!void {
    var slice: [@sizeOf(T)]u8 = undefined;
    std.mem.writeIntSliceBig(T, &slice, n);
    try buf.appendSlice(&slice);
}

fn deserialize_int(comptime T: type, buf: []const u8) DeserializeError!DeserializeValue(T) {
    if (buf.len < @sizeOf(T)) {
        return DeserializeError.DeserializeError;
    }
    const result = std.mem.readIntSlice(T, buf, std.builtin.Endian.Big);
    return DeserializeValue(T){ .result = result, .rest = buf[@sizeOf(T)..] };
}

fn serialize_string(buf: *std.ArrayList(u8), s: []const u8) AllocError!void {
    try serialize_int(usize, buf, s.len);
    try buf.appendSlice(s);
}

fn deserialize_string(buf: []const u8) DeserializeError!DeserializeValue([]const u8) {
    const int_value = try deserialize_int(usize, buf);
    const len = int_value.result;
    const rest = int_value.rest;
    if (rest.len < len) {
        return DeserializeError.DeserializeError;
    }

    return DeserializeValue([]const u8){ .result = rest[0..len], .rest = rest[len..] };
}

fn serialize_column(buf: *std.ArrayList(u8), column: types.ColumnMetadata) AllocError!void {
    try serialize_int(u8, buf, @intFromEnum(column.ty));
    try serialize_string(buf, column.name);
}

fn deserialize_column(buf: []const u8) DeserializeError!DeserializeValue(types.ColumnMetadata) {
    const ty_value = try deserialize_int(u8, buf);
    const ty = @as(types.Type, @enumFromInt(ty_value.result));
    var rest = ty_value.rest;
    const name_value = try deserialize_string(rest);
    const name = name_value.result;
    rest = name_value.rest;
    return DeserializeValue(types.ColumnMetadata){ .result = types.ColumnMetadata{ .name = name, .ty = ty }, .rest = rest };
}

fn serialize_columns(buf: *std.ArrayList(u8), columns: []const types.ColumnMetadata) AllocError!void {
    try serialize_int(usize, buf, columns.len);
    for (columns) |column| {
        try serialize_column(buf, column);
    }
}

pub fn serialize_table_metadata(metadata: types.TableMetadata, allocator: std.mem.Allocator) AllocError!std.ArrayList(u8) {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    try serialize_string(&buf, metadata.name);
    try serialize_columns(&buf, metadata.columns);
    return buf;
}

pub fn deserialize_table_metadata(buf: []const u8, allocator: std.mem.Allocator) DeserializeError!types.TableMetadata {
    const name_value = try deserialize_string(buf);
    const name = name_value.result;
    var rest = name_value.rest;
    const column_count_value = try deserialize_int(usize, rest);
    const column_count = column_count_value.result;
    rest = column_count_value.rest;
    var columns = try std.ArrayList(types.ColumnMetadata).initCapacity(allocator, column_count);
    errdefer columns.deinit();

    for (0..column_count) |i| {
        _ = i;
        const column_value = try deserialize_column(rest);
        rest = column_value.rest;
        try columns.append(column_value.result);
    }

    return types.TableMetadata{ .name = name, .columns = columns.items, .allocator = allocator };
}

test "serialize table metadata" {
    const metadata = types.TableMetadata{
        .name = "foo",
        .columns = &[_]types.ColumnMetadata{
            types.ColumnMetadata{
                .name = "bar",
                .ty = types.Type.Int,
            },
            types.ColumnMetadata{
                .name = "baz",
                .ty = types.Type.Text,
            },
        },
        .allocator = undefined,
    };

    const expected = "\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x03bar\x01\x00\x00\x00\x00\x00\x00\x00\x03baz";
    const result = try serialize_table_metadata(metadata, std.testing.allocator);
    defer result.deinit();
    try testing.expectEqualSlices(u8, expected, result.items);
}

test "deserialize table metadata" {
    const input = "\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x03bar\x01\x00\x00\x00\x00\x00\x00\x00\x03baz";
    var expectedColumns = std.ArrayList(types.ColumnMetadata).init(std.testing.allocator);
    defer expectedColumns.deinit();

    try expectedColumns.append(types.ColumnMetadata{
        .name = "bar",
        .ty = types.Type.Int,
    });

    try expectedColumns.append(types.ColumnMetadata{
        .name = "baz",
        .ty = types.Type.Text,
    });

    const expected = types.TableMetadata{
        .name = "foo",
        .columns = expectedColumns.items,
        .allocator = std.testing.allocator,
    };

    const result = try deserialize_table_metadata(input, std.testing.allocator);
    defer result.deinit();

    try testing.expectEqualDeep(expected, result);
}

test "roundtrip serde" {
    var expectedColumns = std.ArrayList(types.ColumnMetadata).init(std.testing.allocator);
    defer expectedColumns.deinit();

    try expectedColumns.append(types.ColumnMetadata{
        .name = "bar",
        .ty = types.Type.Int,
    });

    try expectedColumns.append(types.ColumnMetadata{
        .name = "baz",
        .ty = types.Type.Text,
    });

    const expected = types.TableMetadata{
        .name = "foo",
        .columns = expectedColumns.items,
        .allocator = std.testing.allocator,
    };

    const serialized = try serialize_table_metadata(expected, std.testing.allocator);
    defer serialized.deinit();

    const result = try deserialize_table_metadata(serialized.items, std.testing.allocator);
    defer result.deinit();

    try testing.expectEqualDeep(expected, result);
}
