const std = @import("std");
const dal = @import("./dal.zig");
const Dal = dal.Dal;
const collection = @import("./collection.zig");
const tx = @import("./transaction.zig");
const db = @import("./db.zig");

pub fn main() !void {
    var gpt = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpt.deinit();
    const allocator = gpt.allocator();

    const path = "libra.db";
    const options = dal.Options{
        .pageSize = std.mem.page_size,
        .minFillPercent = 0.5,
        .maxFillPercent = 1.0,
    };
    const initDal = try Dal.init(allocator, path, options);
    const ldb = db.DB.init(initDal);
    defer ldb.destroy();
    {
        var trx = tx.TX.init(ldb, true);
        defer trx.commit() catch unreachable;
        var c = trx.createCollection("collection1") catch unreachable;
        defer c.deinit();
    }

    {
        var trx = tx.TX.init(ldb, true);
        defer trx.commit();
        var c = trx.getCollection("collection1") catch unreachable;
        defer c.deinit();
    }
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
