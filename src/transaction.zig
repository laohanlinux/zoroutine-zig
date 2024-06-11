const std = @import("std");
const Node = @import("./node.zig").Node;
const Item = @import("./node.zig").Item;
const DB = @import("./db.zig").DB;
const collection = @import("./collection.zig");
const Collection = collection.Collection;

pub const TX = struct {
    // reference the transaction node that be written!
    dirtyNodes: std.AutoHashMap(u64, *Node),
    pagesToDelete: std.ArrayList(u64),

    // new pages allocated during the transaction. They will be released if rollback is called.
    allocatedPageNums: std.ArrayList(u64),
    write: bool,
    db: *DB,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db: *DB, writeable: bool) *Self {
        var self = allocator.create(Self) catch unreachable;
        self.dirtyNodes = std.AutoHashMap(u64, *Node).init(allocator);
        self.pagesToDelete = std.ArrayList(u64).init(allocator);
        self.allocatedPageNums = std.ArrayList(u64).init(allocator);
        self.write = writeable;
        self.db = db;
        self.allocator = allocator;
        return self;
    }

    pub fn destroy(self: *Self) void {
        defer std.log.info("succeed to destroy tx", .{});
        self.allocator.destroy(self);
    }

    pub fn newNode(self: *Self, items: []*const Item, childNodes: []const u64) *Node {
        const node = Node.init(self.allocator);
        node.items.appendSlice(items) catch unreachable;
        node.childNodes.appendSlice(childNodes) catch unreachable;
        node.pageNum = self.db.dal.freelist.getNextPage();
        node.tx = self;
        node.tx.allocatedPageNums.append(node.pageNum) catch unreachable;
        return node;
    }

    pub fn getNode(self: *const Self, pageNum: u64) !*Node {
        // When a new node write to tx, it store at dirtyNodes.
        // So it be searched by pageNum
        if (self.dirtyNodes.get(pageNum)) |node| {
            return node;
        }
        const node = try self.db.dal.getNode() orelse return error.NotFound;
        return node;
    }

    // Write the node with tx, so tx also owns the node
    // This is used to write the node to the db
    pub fn writeNode(self: *Self, node: *Node) *Node {
        self.dirtyNodes.put(node.pageNum, node) catch unreachable;
        node.tx = self;
        return node;
    }

    // Delete the node with tx, so tx also owns the node
    pub fn deleteNode(self: *Self, node: *Node) void {
        self.pagesToDelete.append(node) catch unreachable;
    }

    pub fn rollBack(self: *Self) void {
        if (!self.write) {
            self.db.rwLock.unlockShared();
            return;
        }
        defer self.db.rwLock.unlock();
        self.pagesToDelete.deinit();
        self.allocatedPageNums.deinit();
        for (self.allocatedPageNums.items) |pageNum| {
            // TODO
            self.db.dal.freelist.releasePage(pageNum);
        }
        self.allocatedPageNums.deinit();
        self.dirtyNodes.deinit();
    }

    pub fn commit(self: *Self) !void {
        if (!self.write) {
            self.db.rwLock.unlockShared();
            return;
        }

        var itr = self.dirtyNodes.iterator();
        while (itr.next()) |entry| {
            const node = entry.value_ptr.*;
            _ = try self.db.dal.writeNode(node);
            node.destroy();
        }
        self.dirtyNodes.deinit();

        for (self.pagesToDelete.items) |pageNum| {
            self.db.dal.deleteNode(pageNum);
        }
        self.pagesToDelete.deinit();

        const page = try self.db.dal.writeFreelist();
        page.deinit(self.allocator);

        self.db.rwLock.unlock();
    }

    pub fn getCollection(self: *Self, name: []const u8) *Collection {
        const rootCollection = self._getCollection();
        defer rootCollection.deinit();
        const item = try rootCollection.find(name);
        var _collection = Collection.initEmpty(self.allocator);
        _collection.deseriliaze(item);
        _collection.*.tx = self;

        return _collection;
    }

    pub fn createCollection(self: *Self, name: []const u8) !*Collection {
        if (!self.write) {
            return error.WriteInsideReadTx;
        }

        const newCollectionPage = try self.db.dal.writeNode(Node.init(self.allocator));
        defer newCollectionPage.destroy();
        const newCollection = Collection.initEmpty(self.allocator);
        newCollection.name.? = name;
        newCollection.root = newCollectionPage.*.pageNum;
        newCollection = try self.createCollection(newCollection);
        return newCollection;
    }

    pub fn deleteCollection(self: *Self, name: []const u8) !void {
        if (!self.write) {
            return error.WriteInsideReadTx;
        }

        const rootCollection = self.getCollection(name);
        try rootCollection.remove(name);
    }

    fn _createCollection(self: Self, c: *Collection) !*Collection {
        c.tx.? = self;
        const cBytes = c.serialize();

        const rootCollection = self._getCollection();
        try rootCollection.put(c.name.?, cBytes.value);
        return c;
    }

    fn _getCollection(self: *Self) *Collection {
        const rootCollection = Collection.initEmpty(self.allocator);
        rootCollection.*.root = self.db.dal.meta.root;
        rootCollection.*.tx = self;
        rootCollection.*.counter = 0;
        return rootCollection;
    }
};
