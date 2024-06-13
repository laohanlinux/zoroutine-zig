const std = @import("std");

/// compare returns the order of left and right
pub fn compare(_: void, left: []const u8, right: []const u8) std.math.Order {
    var i: usize = 0;
    while (i < left.len and i < right.len) {
        if (left[i] == right[i]) {} else if (left[i] < right[i]) {
            return std.math.Order.lt;
        } else if (left[i] > right[i]) {
            return std.math.Order.gt;
        }

        i += 1;
    }

    if (left.len == right.len) {
        return std.math.Order.eq;
    } else if (left.len < right.len) {
        return std.math.Order.lt;
    } else {
        return std.math.Order.gt;
    }
}

/// isGte returns true if left >= right
pub fn isGte(left: []const u8, right: []const u8) bool {
    switch (compare({}, left, right)) {
        .gt => return true,
        .eq => return true,
        .lt => return false,
    }
}

pub fn isEq(left: []const u8, right: []const u8) bool {
    switch (compare({}, left, right)) {
        .eq => return true,
        else => return false,
    }
}

pub fn cpBytes(allocator: std.mem.Allocator, s: []const u8) []u8 {
    const bytes = allocator.alloc(u8, s.len) catch unreachable;
    @memcpy(bytes, s);
    return bytes;
}

pub fn stackStr(comptime str: []const u8) []u8 {
    var newString: [str.len]u8 = undefined;
    @memcpy(newString[0..str.len], str[0..str.len]);
    return newString;
}

pub fn stackStrN(comptime n: usize) [n]u8 {
    const newString: [n]u8 = undefined;
    @memset(newString, 0);
    return newString;
}
