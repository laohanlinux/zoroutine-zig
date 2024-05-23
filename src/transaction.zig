const std = @import("std");
const Node = @import("./node.zig").Node;
const DB = @import("./db.zig").DB;

pub const TX = struct {
    dirtyNodes: std.HashMap(u64, *Node, {}, 80),
    pagesToDelete: std.ArrayList(u64),

    // new pages allocated during the transaction. They will be released if rollback is called.
    allocatedPageNums: std.ArrayList(u64),
    write: bool,
    db: *DB,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db: *DB, writeable: bool) *Self {
        var self = allocator.create(Self) catch unreachable;
        self.dirtyNodes = std.HashMap(u64, *Node, {}, 80).init(allocator);
        self.pagesToDelete = std.ArrayList(u64).init(allocator);
        self.allocatedPageNums = std.ArrayList(u64).init(allocator);
        self.write = writeable;
        self.db = db;
        self.allocator = allocator;
        return self;
    }

    pub fn destroy(self: *Self) void {
        self.dirtyNodes.deinit();
        self.pagesToDelete.deinit();
        self.allocatedPageNums.deinit();
        self.allocator.destroy(self);
    }

    pub fn getNode(self: *Self, pageNum: u64) !*Node {
        // Why
        if (self.dirtyNodes.get(pageNum)) |node| {
            return node;
        }
    }

    // Write the node with tx, so tx also owns the node
    // This is used to write the node to the db
    pub fn writeNode(self: *Self, node: *Node) *Node {
        self.dirtyNodes.put(node.pageNum, node) catch unreachable;
        node.tx = self;
        return node;
    }
};
