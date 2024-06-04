const std = @import("std");
const Dal = @import("./dal.zig").Dal;

pub fn main() !void {
    const allocator = std.heap.GeneralPurposeAllocator(.{}).allocator();
    const path = "libra.db";
    const db = try Dal.init(allocator, path, .{});
    defer db.deinit();
    std.log.info("db page's size: {}\n", .{db.pageSize});
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
