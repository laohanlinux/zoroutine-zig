const std = @import("std");

const transaction = @import("./transaction.zig");
const node = @import("./node.zig");
const Dal = @import("./dal.zig").Dal;

pub const DB = struct {
    // Allows only one writer at a time
    rwLock: std.Thread.RwLock,
    dal: *Dal,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(dal: *Dal) *DB {
        var ldb = dal.allocator.create(Self) catch unreachable;
        ldb.rwLock = std.Thread.RwLock{};
        ldb.dal = dal;
        ldb.allocator = dal.allocator;
        return ldb;
    }

    pub fn destroy(self: *Self) void {
        self.dal.deinit();
        self.allocator.destroy(self);
    }

    pub fn readTx(self: *Self) *transaction.TX {
        self.rwLock.lockShared();
        return transaction.TX.init(self.allocator, self, false);
    }

    pub fn writeTx(self: *Self) *transaction.TX {
        self.rwLock.lock();
        return transaction.TX(self.allocator, self, true);
    }
};
