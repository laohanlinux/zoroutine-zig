const std = @import("std");

const transaction = @import("./transaction.zig");
const node = @import("./node.zig");

pub const DB = struct {
    // Allows only one writer at a time
    rwLock: *std.Thread.RwLock,
};
