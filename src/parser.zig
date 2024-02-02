const std = @import("std");
const types = @import("types.zig");
const parser_types = @import("parser_types.zig");
const testing = std.testing;

const Error = types.Error;

pub const Cursor = struct {
    sql: types.String,
    pos: usize,
    allocator: std.mem.Allocator,

    pub fn dump(self: *Cursor) void {
        std.debug.print("\nCursor {{ sql: {s}, pos: {d} }}\n", .{ self.sql[self.pos..], self.pos });
    }

    pub fn new(sql: types.String, allocator: std.mem.Allocator) Cursor {
        return Cursor{
            .sql = sql,
            .pos = 0,
            .allocator = allocator,
        };
    }

    pub fn peek_str(self: *Cursor, n: usize) Error!types.String {
        if ((self.pos + n) > self.sql.len) {
            return Error.ParserError;
        }
        return self.sql[self.pos .. self.pos + n];
    }

    pub fn peek_char(self: *Cursor) Error!u8 {
        if (self.pos >= self.sql.len) {
            return Error.ParserError;
        }
        return self.sql[self.pos];
    }

    pub fn advance(self: *Cursor, n: usize) Error!void {
        if ((self.pos + n) > self.sql.len) {
            return Error.ParserError;
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

    pub fn match_keyword(self: *Cursor, comptime keyword: []const u8) Error!void {
        const peeked = try self.peek_str(keyword.len);
        var buf = [1]u8{0} ** keyword.len;
        var lower_peeked = std.ascii.lowerString(&buf, peeked);
        if (std.mem.eql(u8, lower_peeked, keyword)) {
            try self.advance(keyword.len);
            return;
        }
        return Error.ParserError;
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

    pub fn match_char(self: *Cursor, comptime c: u8) Error!void {
        const peeked = try self.peek_char();
        if (peeked == c) {
            try self.advance(1);
            return;
        }
        return Error.ParserError;
    }

    pub fn consume_while(self: *Cursor, comptime predicate: fn (c: u8) bool) Error!types.String {
        const starting_position = self.pos;
        while (true) {
            if (self.pos == self.sql.len) {
                break;
            }
            var peeked = try self.peek_char();
            if (!predicate(peeked)) {
                break;
            }
            try self.advance(1);
        }
        return self.sql[starting_position..self.pos];
    }

    pub fn match_identifier(self: *Cursor) Error!types.String {
        const start_char = try self.peek_char();
        const starting_position = self.pos;
        if (std.ascii.isAlphabetic(start_char)) {
            try self.advance(1);
            while (true) {
                if (self.pos == self.sql.len) {
                    break;
                }
                const peeked = try self.peek_char();
                if (std.ascii.isAlphabetic(peeked) or std.ascii.isDigit(peeked) or peeked == '_') {
                    try self.advance(1);
                } else {
                    break;
                }
            }
            return self.sql[starting_position..self.pos];
        }
        return Error.ParserError;
    }

    pub fn match_end(self: *Cursor) Error!void {
        if ((self.pos == self.sql.len) or (try self.peek_char() == ';' and self.pos + 1 == self.sql.len)) {
            return;
        }
        return Error.ParserError;
    }
};

fn ParserFunction(comptime T: type) type {
    return fn (cursor: *Cursor) Error!T;
}

fn parse_with_delimiters(comptime T: type, cursor: *Cursor, comptime start: u8, comptime end: u8, comptime parser: ParserFunction(T)) Error!T {
    try cursor.match_char(start);
    cursor.consume_ws();
    const result = try parser(cursor);
    cursor.consume_ws();
    try cursor.match_char(end);
    return result;
}

fn parse_list(comptime T: type, cursor: *Cursor, comptime delim: u8, comptime parser: ParserFunction(T)) Error!std.ArrayList(T) {
    var result = std.ArrayList(T).init(cursor.allocator);
    while (true) {
        cursor.consume_ws();
        const item = try parser(cursor);
        try result.append(item);
        cursor.consume_ws();
        cursor.match_char(delim) catch {
            break;
        };
    }
    return result;
}

fn try_parser(comptime T: type, cursor: *Cursor, comptime parser: ParserFunction(T)) ?T {
    const initial_pos = cursor.pos;
    return parser(cursor) catch {
        cursor.pos = initial_pos;
        return null;
    };
}

fn parse_identifiers(cursor: *Cursor) Error!std.ArrayList(types.String) {
    return try parse_list(types.String, cursor, ',', Cursor.match_identifier);
}

fn is_not_single_quote(c: u8) bool {
    return c != '\'';
}

fn parse_value(cursor: *Cursor) Error!types.Value {
    if (cursor.try_match_keyword("'")) {
        const value = try cursor.consume_while(is_not_single_quote);
        try cursor.match_char('\'');
        return types.Value{ .Text = value };
    } else if (std.ascii.isDigit(try cursor.peek_char())) {
        const value = try cursor.consume_while(std.ascii.isDigit);
        const int_value = std.fmt.parseInt(i64, value, 10) catch {
            return Error.ParserError;
        };
        return types.Value{ .Int = int_value };
    } else {
        return Error.ParserError;
    }
}

fn parse_values(cursor: *Cursor) Error!std.ArrayList(types.Value) {
    return try parse_list(types.Value, cursor, ',', parse_value);
}

fn parse_names(cursor: *Cursor) Error!std.ArrayList(types.String) {
    cursor.consume_ws();
    return try parse_with_delimiters(std.ArrayList(types.String), cursor, '(', ')', parse_identifiers);
}

fn parse_column(cursor: *Cursor) Error!parser_types.Column {
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
        return Error.ParserError;
    }
}

fn parse_columns(cursor: *Cursor) Error!std.ArrayList(parser_types.Column) {
    return parse_list(parser_types.Column, cursor, ',', parse_column);
}

pub fn parse_create_statement(cursor: *Cursor) Error!parser_types.CreateStatement {
    cursor.consume_ws();
    try cursor.match_keyword("table");
    cursor.consume_ws();
    const name = try cursor.match_identifier();
    cursor.consume_ws();
    const columns = try parse_with_delimiters(std.ArrayList(parser_types.Column), cursor, '(', ')', parse_columns);
    errdefer columns.deinit();

    cursor.consume_ws();
    try cursor.match_end();

    return parser_types.CreateStatement{
        .name = name,
        .columns = columns,
    };
}

pub fn parse_insert_statement(cursor: *Cursor) Error!parser_types.InsertStatement {
    cursor.consume_ws();
    try cursor.match_keyword("into");
    cursor.consume_ws();
    const name = try cursor.match_identifier();

    const names = try_parser(std.ArrayList(types.String), cursor, parse_names);
    errdefer if (names) |n| n.deinit();

    cursor.consume_ws();
    try cursor.match_keyword("values");
    cursor.consume_ws();
    const values = try parse_with_delimiters(std.ArrayList(types.Value), cursor, '(', ')', parse_values);
    errdefer values.deinit();

    cursor.consume_ws();
    try cursor.match_end();

    return parser_types.InsertStatement{
        .name = name,
        .column_names = names,
        .values = values,
    };
}

fn parse_atom(cursor: *Cursor) Error!parser_types.Atom {
    if (cursor.try_match_keyword("'")) {
        const value = try cursor.consume_while(is_not_single_quote);
        try cursor.match_char('\'');
        return parser_types.Atom{ .StringLiteral = value };
    } else if (std.ascii.isDigit(try cursor.peek_char())) {
        const value = try cursor.consume_while(std.ascii.isDigit);
        const int_value = std.fmt.parseInt(i64, value, 10) catch {
            return Error.ParserError;
        };
        return parser_types.Atom{ .IntLiteral = int_value };
    } else {
        const ident = try cursor.match_identifier();
        return parser_types.Atom{ .Identifier = ident };
    }
}

fn parse_comparison_operator(cursor: *Cursor) Error!parser_types.ComparisonOp {
    if (cursor.try_match_keyword("=")) {
        return parser_types.ComparisonOp.Eq;
    } else if (cursor.try_match_keyword("<")) {
        return parser_types.ComparisonOp.Lt;
    } else if (cursor.try_match_keyword(">")) {
        return parser_types.ComparisonOp.Gt;
    }
    return Error.ParserError;
}

fn parse_where_clause(cursor: *Cursor) Error!parser_types.Expression {
    cursor.consume_ws();
    try cursor.match_keyword("where");
    cursor.consume_ws();
    const atom1 = try parse_atom(cursor);
    cursor.consume_ws();
    const op = try parse_comparison_operator(cursor);
    cursor.consume_ws();
    const atom2 = try parse_atom(cursor);

    return parser_types.Expression{
        .Comparison = parser_types.Comparison{
            .lhs = atom1,
            .rhs = atom2,
            .op = op,
        },
    };
}

fn parse_select_statement(cursor: *Cursor) Error!parser_types.SelectStatement {
    cursor.consume_ws();
    const columns = if (cursor.try_match_keyword("*")) blk: {
        break :blk null;
    } else blk: {
        break :blk try parse_identifiers(cursor);
    };
    errdefer if (columns) |c| c.deinit();

    cursor.consume_ws();
    try cursor.match_keyword("from");
    cursor.consume_ws();
    const table = try cursor.match_identifier();
    const where_clause = try_parser(parser_types.Expression, cursor, parse_where_clause);
    cursor.consume_ws();
    try cursor.match_end();

    return parser_types.SelectStatement{
        .column_names = columns,
        .name = table,
        .where_clause = where_clause,
    };
}

pub fn parse_statement(cursor: *Cursor) Error!parser_types.Statement {
    cursor.consume_ws();
    if (cursor.try_match_keyword("create")) {
        return parser_types.Statement{ .Create = try parse_create_statement(cursor) };
    } else if (cursor.try_match_keyword("insert")) {
        return parser_types.Statement{ .Insert = try parse_insert_statement(cursor) };
    } else if (cursor.try_match_keyword("select")) {
        return parser_types.Statement{ .Select = try parse_select_statement(cursor) };
    }
    return Error.ParserError;
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

test "parse insert statement with names" {
    var expectedValues = std.ArrayList(types.Value).init(std.testing.allocator);
    defer expectedValues.deinit();
    try expectedValues.append(types.Value{ .Text = "foo" });
    try expectedValues.append(types.Value{ .Int = 42 });
    var expectedNames = std.ArrayList(types.String).init(std.testing.allocator);
    defer expectedNames.deinit();
    try expectedNames.append("name");
    try expectedNames.append("age");

    const expectedResult = parser_types.Statement{ .Insert = parser_types.InsertStatement{
        .name = "foo",
        .values = expectedValues,
        .column_names = expectedNames,
    } };
    var cursor = Cursor{
        .sql = "INSERT INTO foo (name, age) VALUES ('foo', 42);",
        .pos = 0,
        .allocator = std.testing.allocator,
    };
    const result = try parse_statement(&cursor);
    defer result.deinit();
    try testing.expectEqualDeep(expectedResult, result);
}

test "parse insert statement without names" {
    var expectedValues = std.ArrayList(types.Value).init(std.testing.allocator);
    defer expectedValues.deinit();
    try expectedValues.append(types.Value{ .Text = "foo" });
    try expectedValues.append(types.Value{ .Int = 42 });

    const expectedResult = parser_types.Statement{ .Insert = parser_types.InsertStatement{
        .name = "foo",
        .values = expectedValues,
        .column_names = null,
    } };
    var cursor = Cursor{
        .sql = "INSERT INTO foo VALUES ('foo', 42);",
        .pos = 0,
        .allocator = std.testing.allocator,
    };
    const result = try parse_statement(&cursor);
    defer result.deinit();
    try testing.expectEqualDeep(expectedResult, result);
}

test "parse select statement without where clause" {
    var expectedColumns = std.ArrayList(types.String).init(std.testing.allocator);
    defer expectedColumns.deinit();
    try expectedColumns.append("name");
    try expectedColumns.append("age");

    const expectedResult = parser_types.Statement{ .Select = parser_types.SelectStatement{
        .column_names = expectedColumns,
        .name = "foo",
        .where_clause = null,
    } };
    var cursor = Cursor{
        .sql = "SELECT name, age FROM foo;",
        .pos = 0,
        .allocator = std.testing.allocator,
    };
    const result = try parse_statement(&cursor);
    defer result.deinit();
    try testing.expectEqualDeep(expectedResult, result);
}

test "parse select statement with where clause" {
    var expectedColumns = std.ArrayList(types.String).init(std.testing.allocator);
    defer expectedColumns.deinit();
    try expectedColumns.append("name");
    try expectedColumns.append("age");

    const expectedResult = parser_types.Statement{ .Select = parser_types.SelectStatement{
        .column_names = expectedColumns,
        .name = "foo",
        .where_clause = parser_types.Expression{
            .Comparison = parser_types.Comparison{
                .lhs = parser_types.Atom{ .Identifier = "age" },
                .rhs = parser_types.Atom{ .IntLiteral = 42 },
                .op = parser_types.ComparisonOp.Eq,
            },
        },
    } };
    var cursor = Cursor{
        .sql = "SELECT name, age FROM foo WHERE age = 42",
        .pos = 0,
        .allocator = std.testing.allocator,
    };
    const result = try parse_statement(&cursor);
    defer result.deinit();
    try testing.expectEqualDeep(expectedResult, result);
}

test "parse select * without where clause" {
    const expectedResult = parser_types.Statement{ .Select = parser_types.SelectStatement{
        .column_names = null,
        .name = "foo",
        .where_clause = null,
    } };
    var cursor = Cursor{
        .sql = "SELECT * FROM foo",
        .pos = 0,
        .allocator = std.testing.allocator,
    };
    const result = try parse_statement(&cursor);
    defer result.deinit();
    try testing.expectEqualDeep(expectedResult, result);
}
