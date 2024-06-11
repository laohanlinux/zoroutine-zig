const std = @import("std");
const dal = @import("./dal.zig");
const Dal = dal.Dal;
const collection = @import("./collection.zig");

pub fn main() !void {
    var gpt = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpt.deinit();
    const allocator = gpt.allocator();

    const path = "libra.db";
    const options = dal.Options{
        .pageSize = std.mem.page_size,
        .minFillPercent = 0.0125,
        .maxFillPercent = 0.025,
    };
    const db = try Dal.init(allocator, path, options);
    defer db.deinit();
    std.log.info("db page's size: {}\n", .{db.pageSize});

    var c = collection.Collection.init(allocator, "collection1", db.meta.root);
    defer c.deinit();
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
