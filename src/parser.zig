const std = @import("std");
const types = @import("types.zig");
const parser_types = @import("parser_types.zig");
const testing = std.testing;

const Cursor = struct {
    sql: types.String,
    pos: usize,
    allocator: std.mem.Allocator,

    pub fn peek_str(self: *Cursor, n: usize) error{ParserError}!types.String {
        if ((self.pos + n) > self.sql.len) {
            return error.ParserError;
        }
        return self.sql[self.pos .. self.pos + n];
    }

    pub fn peek_char(self: *Cursor) error{ParserError}!u8 {
        if (self.pos >= self.sql.len) {
            return error.ParserError;
        }
        return self.sql[self.pos];
    }

    pub fn advance(self: *Cursor, n: usize) error{ParserError}!void {
        if ((self.pos + n) > self.sql.len) {
            return error.ParserError;
        }

        self.pos += n;
    }

    pub fn consume_ws(self: *Cursor) void {
        while (true) {
            const peeked = self.peek_char() catch {
                return;
            };
            if (std.ascii.isWhitespace(peeked)) {
                self.pos += 1;
                continue;
            }
            break;
        }
    }

    pub fn match_keyword(self: *Cursor, comptime keyword: []const u8) error{ParserError}!void {
        const peeked = try self.peek_str(keyword.len);
        var buf = [1]u8{0} ** keyword.len;
        var lower_peeked = std.ascii.lowerString(&buf, peeked);
        if (std.mem.eql(u8, lower_peeked, keyword)) {
            try self.advance(keyword.len);
            return;
        }
        return error.ParserError;
    }

    pub fn try_match_keyword(self: *Cursor, comptime keyword: []const u8) bool {
        const peeked = self.peek_str(keyword.len) catch {
            return false;
        };
        var buf = [1]u8{0} ** keyword.len;
        var lower_peeked = std.ascii.lowerString(&buf, peeked);
        if (std.mem.eql(u8, lower_peeked, keyword)) {
            self.advance(keyword.len) catch {
                unreachable;
            };
            return true;
        }
        return false;
    }

    pub fn match_char(self: *Cursor, comptime c: u8) error{ParserError}!void {
        const peeked = try self.peek_char();
        if (peeked == c) {
            try self.advance(1);
            return;
        }
        return error.ParserError;
    }

    pub fn match_identifier(self: *Cursor) error{ParserError}!types.String {
        const start_char = try self.peek_char();
        const starting_position = self.pos;
        if (std.ascii.isAlphabetic(start_char)) {
            try self.advance(1);
            while (true) {
                const peeked = try self.peek_char();
                if (std.ascii.isAlphabetic(peeked) or std.ascii.isDigit(peeked) or peeked == '_') {
                    try self.advance(1);
                } else if (std.ascii.isWhitespace(peeked)) {
                    break;
                } else {
                    return error.ParserError;
                }
            }
            return self.sql[starting_position..self.pos];
        }
        return error.ParserError;
    }
};

fn ParserFunction(comptime T: type) type {
    return fn (cursor: *Cursor) error{ParserError}!T;
}

fn parse_with_delimiters(comptime T: type, cursor: *Cursor, comptime start: u8, comptime end: u8, comptime parser: ParserFunction(T)) error{ParserError}!T {
    try cursor.match_char(start);
    cursor.consume_ws();
    const result = try parser(cursor);
    cursor.consume_ws();
    try cursor.match_char(end);
    return result;
}

fn parse_list(comptime T: type, cursor: *Cursor, comptime delim: u8, comptime parser: ParserFunction(T)) error{ParserError}!std.ArrayList(T) {
    var result = std.ArrayList(T).init(cursor.allocator);
    while (true) {
        cursor.consume_ws();
        const item = try parser(cursor);
        result.append(item) catch {
            unreachable;
        };
        cursor.consume_ws();
        cursor.match_char(delim) catch {
            break;
        };
    }
    return result;
}

fn parse_column(cursor: *Cursor) error{ParserError}!parser_types.Column {
    const name = try cursor.match_identifier();
    cursor.consume_ws();

    if (cursor.try_match_keyword("text")) {
        return parser_types.Column{
            .name = name,
            .ty = types.Type.Text,
        };
    } else if (cursor.try_match_keyword("int")) {
        return parser_types.Column{
            .name = name,
            .ty = types.Type.Int,
        };
    } else {
        return error.ParserError;
    }
}

fn parse_columns(cursor: *Cursor) error{ParserError}!std.ArrayList(parser_types.Column) {
    return parse_list(parser_types.Column, cursor, ',', parse_column);
}

pub fn parse_create_statement(cursor: *Cursor) error{ParserError}!parser_types.CreateStatement {
    cursor.consume_ws();
    try cursor.match_keyword("table");
    cursor.consume_ws();
    const name = try cursor.match_identifier();
    cursor.consume_ws();
    const columns = try parse_with_delimiters(std.ArrayList(parser_types.Column), cursor, '(', ')', parse_columns);

    return parser_types.CreateStatement{
        .name = name,
        .columns = columns,
    };
}

pub fn parse_statement(cursor: *Cursor) error{ParserError}!parser_types.Statement {
    cursor.consume_ws();
    if (cursor.try_match_keyword("create")) {
        return parser_types.Statement{ .Create = try parse_create_statement(cursor) };
    }
    return error.ParserError;
}

test "parse create statement" {
    var expectedColumns = std.ArrayList(parser_types.Column).init(std.testing.allocator);
    defer expectedColumns.deinit();
    try expectedColumns.append(parser_types.Column{
        .name = "name",
        .ty = types.Type.Text,
    });
    try expectedColumns.append(parser_types.Column{
        .name = "age",
        .ty = types.Type.Int,
    });
    const expectedResult = parser_types.Statement{ .Create = parser_types.CreateStatement{
        .name = "foo",
        .columns = expectedColumns,
    } };
    var cursor = Cursor{
        .sql = "CREATE TABLE foo (name TEXT, age INT);",
        .pos = 0,
        .allocator = std.testing.allocator,
    };
    const result = try parse_statement(&cursor);
    defer result.deinit();
    try testing.expectEqualDeep(expectedResult, result);
}
