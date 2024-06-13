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

    pub fn init(db: *DB, writeable: bool) *Self {
        var self = db.allocator.create(Self) catch unreachable;
        self.dirtyNodes = std.AutoHashMap(u64, *Node).init(db.allocator);
        self.pagesToDelete = std.ArrayList(u64).init(db.allocator);
        self.allocatedPageNums = std.ArrayList(u64).init(db.allocator);
        self.write = writeable;
        self.db = db;
        self.allocator = db.allocator;
        if (writeable) {
            self.db.rwLock.lock();
        } else {
            self.db.rwLock.lockShared();
        }
        return self;
    }

    pub fn deinit(self: *Self) void {
        var itr = self.dirtyNodes.iterator();
        while (itr.next()) |entry| {
            entry.value_ptr.*.*.destroy();
        }
        self.dirtyNodes.deinit();
        self.pagesToDelete.deinit();
        self.allocatedPageNums.deinit();
    }

    pub fn newNode(self: *Self, items: []*Item, childNodes: ?[]const u64) *Node {
        var node = Node.init(self.allocator);
        node.items.appendSlice(items) catch unreachable;
        if (childNodes) |cNodes| {
            node.childNodes.appendSlice(cNodes) catch unreachable;
        }
        node.pageNum = self.db.dal.freelist.getNextPage();
        node.tx = self;
        // When a new node write to tx, it store at allocatedPageNums.
        node.tx.?.allocatedPageNums.append(node.pageNum) catch unreachable;
        return node;
    }

    /// Return the node with the given pageNum.
    pub fn getNode(self: *Self, pageNum: u64) !*Node {
        if (self.dirtyNodes.get(pageNum)) |node| {
            return node;
        }
        const node = try self.db.dal.getNode(pageNum);
        node.tx = self;
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

    // Rollback the transaction
    pub fn rollBack(self: *Self) void {
        if (!self.write) {
            self.deinit();
            self.db.rwLock.unlockShared();
            self.allocator.destroy(self);
            return;
        }
        defer self.allocator.destroy(self);
        defer self.db.rwLock.unlock();
        for (self.allocatedPageNums.items) |pageNum| {
            // Because we have allocated the page, we need to release it for the next use, avoid memory leak.
            self.db.dal.freelist.releasePage(pageNum);
        }
        self.deinit();
    }

    // Commit the transaction
    pub fn commit(self: *Self) !void {
        std.log.debug("ready to commit!", .{});
        defer std.log.debug("after commit!!!", .{});
        if (!self.write) {
            self.deinit();
            defer self.allocator.destroy(self);
            self.db.rwLock.unlockShared();
            return;
        }

        // Write the dirty nodes to the db (any modifications to the nodes are written to the db here)
        var itr = self.dirtyNodes.iterator();
        while (itr.next()) |entry| {
            var node = entry.value_ptr.*;
            node = try self.db.dal.writeNode(node);
            // Deinit the node, because the node is written to the db, we don't need to keep it in the tx.
            node.destroy();
        }
        self.dirtyNodes.deinit();

        // Delete the nodes that are marked for deletion
        for (self.pagesToDelete.items) |pageNum| {
            // Release the page, because the page is deleted, we need to release it for the next use, avoid memory leak.
            self.db.dal.deleteNode(pageNum);
        }
        self.pagesToDelete.deinit();

        // Write the freelist to the db
        const page = try self.db.dal.writeFreelist();
        page.deinit(self.allocator);

        // TODO why not update meta node...
        defer self.allocator.destroy(self);
        self.db.rwLock.unlock();
    }

    // Get the root collection
    pub fn getCollection(self: *Self, name: []const u8) !Collection {
        var rootCollection = self.getRootCollection();
        defer rootCollection.deinit();
        var item = try rootCollection.find(name);
        defer item.destroy();
        var _collection = Collection.createEmpty(self.allocator);
        _collection.deseriliaze(item);
        _collection.tx = self;
        return _collection;
    }

    pub fn createCollection(self: *Self, name: []const u8) !Collection {
        if (!self.write) {
            return error.WriteInsideReadTx;
        }
        // allocate a new page for the collection
        var newCollectionPage = try self.db.dal.writeNode(Node.init(self.allocator));
        defer newCollectionPage.destroy();
        var newCollection = Collection.init(self.allocator);
        newCollection.fillName(name);
        newCollection.root = newCollectionPage.*.pageNum;
        try self._createCollection(&newCollection);
        return newCollection;
    }

    pub fn deleteCollection(self: *Self, name: []const u8) !void {
        if (!self.write) {
            return error.WriteInsideReadTx;
        }
        const rootCollection = self.getRootCollection(name);
        try rootCollection.remove(name);
    }

    fn _createCollection(self: *Self, c: *Collection) !void {
        c.tx = self;
        const cBytes = c.serialize();
        defer cBytes.destroy();
        // every collection are locate at Root Collection low
        var rootCollection = self.getRootCollection();
        try rootCollection.put(c.name.?, cBytes.value);
        defer rootCollection.destroy();
    }

    fn getRootCollection(self: *Self) Collection {
        var rootCollection = Collection.init(self.allocator);
        rootCollection.root = self.db.dal.meta.root;
        rootCollection.tx = self;
        return rootCollection;
    }
};
