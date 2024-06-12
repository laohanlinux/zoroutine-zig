const std = @import("std");
const Tx = @import("./transaction.zig").TX;
const Item = @import("./node.zig").Item;
const Node = @import("./node.zig").Node;
const emptyItems = @import("./node.zig").zeroItems;
const util = @import("./util.zig");

pub const Collection = struct {
    name: ?[]u8,
    root: u64,
    counter: u64,
    // associated transaction
    tx: ?*Tx,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub const collectionSize: usize = 16;

    pub fn init(allocator: std.mem.Allocator, name: []const u8, root: u64) *Collection {
        var c = allocator.create(Self) catch unreachable;
        c.allocator = allocator;
        c.name.? = c.allocator.alloc(u8, name.len) catch unreachable;
        @memcpy(c.name.?, name);
        c.root = root;
        return c;
    }

    pub fn initEmpty(allocator: std.mem.Allocator) *Collection {
        var c = allocator.create(Self) catch unreachable;
        c.allocator = allocator;
        c.name = null;
        c.root = 0;
        c.counter = 0;
        c.tx = null;
        return c;
    }

    pub fn deinit(self: *Self) void {
        defer std.log.debug("after collection destroy!", .{});
        std.log.debug("ready collection destroy {any}!", .{self.name == null});
        if (self.name) |name| {
            self.allocator.free(name);
        }
        self.tx = null;
        self.allocator.destroy(self);
    }

    pub fn id(self: *const Self) u64 {
        if (!self.tx.write) {
            return 0;
        }

        const _id = self.counter;
        self.counter += 1;
        return _id;
    }

    pub fn fillName(self: *Self, name: []const u8) void {
        std.debug.assert(self.name == null);
        self.name = util.cpBytes(self.allocator, name);
        std.debug.assert(std.mem.eql(u8, self.name.?, name));
    }

    pub fn serialize(self: *Self) *Item {
        const b = self.allocator.alloc(u8, Self.collectionSize) catch unreachable;
        @memset(b, 0);

        var leftPos: usize = 0;
        std.mem.writeInt(u64, b[leftPos..(leftPos + 8)][0..8], self.root, std.builtin.Endian.big);
        leftPos += 8;

        std.mem.writeInt(u64, b[leftPos..(leftPos + 8)][0..8], self.counter, std.builtin.Endian.big);
        leftPos += 8;

        const name = util.cpBytes(self.allocator, self.name.?);
        const item = Item.init(self.allocator, name, b);
        return item;
    }

    pub fn deseriliaze(self: *Self, item: *const Item) void {
        std.log.info("the item key size: ", .{});
        //self.name.? = self.allocator.alloc(u8, item.key.len);
        //@memcpy(self.name.?, item.key);
        if (item.value.len > 0) {
            var leftPos: usize = 0;
            self.root = std.mem.readVarInt(u64, item.value[leftPos..], std.builtin.Endian.big);
            leftPos += 8;

            self.counter = std.mem.readVarInt(u64, item.value[leftPos..], std.builtin.Endian.big);
        }
    }

    /// Adds a key to the tree. It finds the correct node and the insertion index and adds the item. When performing the
    /// search, the ancestores are returned as well. This way we can iterate over them to check which nodes were modified and
    /// rebalance by splitting them accordingly. If the root has too many items, then a new root of a new layer is
    /// created and the created nodes from the split are added as children.
    pub fn put(self: *Self, key: []u8, value: []u8) !void {
        if (!self.tx.?.write) {
            return error.WriteInsideReadTx;
        }
        const cpKey = try self.allocator.alloc(u8, key.len);
        @memcpy(cpKey, key);
        const cpValue = try self.allocator.alloc(u8, value.len);
        @memcpy(cpValue, value);
        const item = Item.init(self.allocator, cpKey, cpValue);

        // On first insertion the root node does not exist, so it should be created
        var _root: *Node = undefined;
        if (self.root == 0) {
            var items = std.ArrayList(*Item).init(self.allocator);
            items.append(item) catch unreachable;
            defer items.deinit();
            _root = self.tx.?.writeNode(self.tx.?.newNode(items.toOwnedSlice() catch unreachable, null));
            self.root = _root.pageNum;
            _root.destroy();
            return;
        } else {
            std.log.info("Come on bat, root: {}!", .{self.root});
            _root = try self.tx.?.getNode(self.root);
        }

        std.log.info("find {s}, {s}", .{ item.key, item.value });
        // Find the path to the node where the insertion should happen
        const _find = try _root.findKey(item.key, false);
        const insertionIndex: usize = _find[0] orelse return error.NotFound;
        const nodeToInsertIn: *Node = _find[1];
        const ancestorsIndexes = _find[2];
        defer ancestorsIndexes.deinit();

        // If key has already exists
        if (nodeToInsertIn.items.items.len < insertionIndex and util.isEq(nodeToInsertIn.items.items[insertionIndex].key, key)) {
            nodeToInsertIn.items.items[insertionIndex] = item;
        } else {
            // Because find key with exact=false, so if not found the key, it also return a good index of node and the node is **leaf node**.
            // Add item to the leaf node
            _ = nodeToInsertIn.addItem(item, insertionIndex);
        }
        _ = nodeToInsertIn.writeNode(nodeToInsertIn);

        const ancestors = try self.getNodes(ancestorsIndexes.items);
        defer self.allocator.free(ancestors);

        // Rebalance the nodes all the way up. Start from one node before the last and go all the way up. Exclude root.
        var i = ancestors.len - 1;
        while (i > 0) : (i -= 1) {
            const pNode = ancestors[i - 1];
            const node = ancestors[i];
            if (node.isOverPopulated()) {
                try pNode.split(node, ancestorsIndexes.items[i]);
            }
        }

        // Handle root
        const rootNode = ancestors[0];
        if (_root.isOverPopulated()) {
            var childrenNodes = [_]u64{rootNode.pageNum};
            var newRoot = self.tx.?.newNode(emptyItems[0..], childrenNodes[0..]);
            try newRoot.split(rootNode, 0);

            // Commit newly created root
            newRoot = self.tx.?.writeNode(newRoot);
            std.log.info("change root pageid from {any} to {any}", .{ self.root, newRoot.pageNum });
            self.root = newRoot.pageNum;
        }
        return;
    }

    // Returns an item according based on the given key by performing a binary search.
    pub fn find(self: *const Self, key: []const u8) !*Item {
        var node = try self.tx.?.getNode(self.root);
        defer node.destroy();

        const _find = try node.findKey(key, true);
        const index = _find[0] orelse return error.NotFound;
        defer _find[1].destroy();
        return _find[1].items.items[index];
    }

    // Removes a key from the tree. It finds the correct node and the index to remove the item from and removes it.
    // When performing the search, the ancestors are returned as well. This way we can iterate over them to check which
    // nodes were modified and rebalance by rotating or merging the unbalanced nodes. Rotation is done first. If the
    // siblings don't have enough items, then merging occurs. If the root is without items after a split, then the root is
    // removed and the tree is one level shorter.
    pub fn remove(self: *Self, key: []const u8) !void {
        if (!self.tx.?.write) {
            return error.WriteInsideReadTx;
        }

        // Find the path to the node where the deletion should happen
        var rootNode = try self.tx.?.getNode(self.root);
        const _find = try rootNode.findKey(key, true);

        const removeItemIndex: usize = _find[0] orelse return;
        var nodeToRemoveFrom: *Node = _find[1];
        var ancestorsIndexes: []usize = _find[2];
        if (nodeToRemoveFrom.isLeaf()) {
            nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex);
        } else {
            const affectedNodes = try nodeToRemoveFrom.removeItemFromInternal(removeItemIndex);
            ancestorsIndexes = ancestorsIndexes ++ affectedNodes;
        }

        const ancestors = try self.getNodes(ancestorsIndexes);
        defer self.allocator.free(ancestors);

        // Rebalance the nodes all the way up. Start From one node before the last and go all the way up. Exclude root.
        var i = ancestors.len - 1;
        while (i > 0) : (i -= 1) {
            const pNode = ancestors[i - 1];
            const node = ancestors[i];
            if (node.isUnderPopulated()) {
                try pNode.rebalanceRemove(node, ancestorsIndexes[i]);
            }
        }

        rootNode = ancestors[0];

        // If the root has no items after rebalancing, there's no need to save it because we ignore it.
        if (rootNode.items.items.len == 0 and rootNode.childNodes.items.len > 0) {
            self.root = ancestors[1].pageNum;
        }
    }

    // Returns a list of nodes based on their indexes (the breadcrumbs) from the root
    //           p
    //       /       \
    //     a          b
    //  /     \     /   \
    // c       d   e     f
    // For [0,1,0] -> p,b,e
    pub fn getNodes(self: *const Self, indexes: []const usize) ![]*Node {
        const _root = try self.tx.?.getNode(self.root);
        var nodes = try std.ArrayList(*Node).initCapacity(self.allocator, indexes.len + 1);
        try nodes.append(_root);

        var child = nodes.items[0];
        for (indexes) |index| {
            child = try self.tx.?.getNode(child.childNodes.items[index]);
            try nodes.append(child);
        }

        return nodes.items;
    }
};
