const std = @import("std");
const Tx = @import("./transaction.zig").TX;
const Item = @import("./node.zig").Item;
const Node = @import("./node.zig").Node;
const util = @import("./util.zig");

pub const Collection = struct {
    name: []const u8,
    root: u64,
    counter: u64,
    // associated transaction
    tx: *Tx,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub const collectionSize: usize = 16;

    pub fn init(allocator: std.mem.Allocator, name: []const u8, root: u64) *Collection {
        var c = allocator.create(Self) catch unreachable;
        c.name = allocator.create(u8, name.len) catch unreachable;
        std.mem.copyForwards(u8, c.name, name);
        c.root = root;
        c.allocator = allocator;
        return c;
    }

    pub fn initEmpty(allocator: std.mem.Allocator) *Collection {
        var c = try allocator.create(Self);
        c.allocator = allocator;
        return c;
    }

    pub fn deinit(self: *Self) void {
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

    pub fn serialize(self: *Self) *Item {
        const b = self.allocator.alloc(u8, Self.collectionSize) catch unreachable;
        @memset(b, 0);

        var leftPos: usize = 0;
        std.mem.writeInt(u64, b[leftPos..(leftPos + 8)][0..8], self.root, std.builtin.Endian.big);
        leftPos += 8;

        std.mem.writeInt(u64, b[leftPos..(leftPos + 8)][0..8], self.counter, std.builtin.Endian.big);
        leftPos += 8;

        const item = Item.init(self.allocator, self.name, b);
        return item;
    }

    pub fn deseriliaze(self: *Self, item: *const Item) void {
        self.name = item.key;
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
        if (!self.tx.write) {
            return error.WriteInsideReadTx;
        }

        const item = Item.init(self.allocator, key, value);

        // On first insertion the root node does not exist, so it should be created
        var _root: *Node = undefined;
        if (self.root == 0) {
            _root = self.tx.writeNode(self.tx.newNode([_]*Item{item}, null));
            self.root = _root.pageNum;
            _root.destroy();
            return;
        } else {
            _root = try self.tx.getNode(self.root);
        }

        // Find the path to the node where the insertion should happen
        const find = try _root.findKey(item.key, false);
        const insertionIndex: ?usize = find[0];
        const nodeToInsertIn: *Node = find[1];
        const ancestorsIndexes = find[2];
        defer ancestorsIndexes.deinit();

        // If key has already exists
        if (nodeToInsertIn.items.items.len < insertionIndex and util.isEq(.{}, nodeToInsertIn.items.items[insertionIndex].key, key)) {
            nodeToInsertIn.items[insertionIndex] = item;
        } else {
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
            if (node.isUnderPopulated()) {
                try pNode.rebalance(node, ancestorsIndexes[i]);
            }
        }

        _root = ancestors[0];
        // If the root has no items after rebalancing, there's no need to save it because we ignore it.
        if (_root.items.items.len == 0 and _root.childNodes.items.len > 0) {
            self.root = ancestors[1].pageNum;
        }

        return;
    }

    // getNodes returns a list of nodes based on their indexes (the breadcrumbs) from the root
    //           p
    //       /       \
    //     a          b
    //  /     \     /   \
    // c       d   e     f
    // For [0,1,0] -> p,b,e
    pub fn getNodes(self: *const Self, indexes: []const usize) ![]*Node {
        const _root = try self.tx.getNode(self.root);
        var nodes = try self.allocator.alloc(*Node, indexes.len + 1);
        @memset(nodes, null);
        nodes[0] = _root;
        var child = nodes[0];
        for (indexes, 0..) |index, i| {
            child = try self.tx.getNode(child.childNodes.items[index]);
            nodes[i + 1] = child;
        }

        return nodes;
    }
};
