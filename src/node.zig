const std = @import("std");
const transaction = @import("./transaction.zig");

pub const Item = struct {
    key: []u8,
    value: []u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, key: []u8, value: []u8) *Item {
        var item = allocator.create(Item) catch unreachable;
        item.allocator = allocator;
        item.key = key;
        item.value = value;

        return item;
    }

    pub fn destroy(self: *Item) void {
        self.allocator.free(self.key);
        self.allocator.free(self.value);
        self.allocator.destroy(self);
    }
};

pub const Node = struct {
    tx: ?*transaction.TX,
    pageNum: u64,

    items: std.ArrayList(*Item),
    childNodes: std.ArrayList(u64),

    allocator: std.mem.Allocator,

    const Self = @This();
    const nodeHeaderSize: usize = 3;

    pub fn init(allocator: std.mem.Allocator) *Self {
        var self: *Self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.childNodes = std.ArrayList(u64).init(allocator);
        self.items = std.ArrayList(*Item).init(allocator);
        self.tx = null;
    }

    pub fn destroy(self: *Self) void {
        for (self.items.items) |item| {
            item.destroy();
        }
        self.items.deinit();
        self.childNodes.deinit();
        self.allocator.destroy(self);
    }

    pub fn isLeaf(self: *const Self) bool {
        return (self.childNodes.items.len == 0);
    }

    pub fn isLast(parentNode: *const Node, index: usize) bool {
        return (parentNode.items.items.len == index);
    }

    pub fn isFirst(index: usize) bool {
        return (index == 0);
    }

    // Related to transaction with node
    fn writeNode(self: *Self, node: *Node) void {
        return self.tx.?.writeNode(node);
    }

    fn writeNodes(self: *Self, nodes: []*Node) void {
        for (nodes) |node| {
            self.tx.?.writeNode(node);
        }
    }

    /// Get pageNum's node from the transaction
    fn getNode(self: *Self, pageNum: u64) !*Node {
        return self.tx.?.getNode(pageNum);
    }

    /// isOverPopulated checks if the node size is bigger than the size of a page.
    fn isOverPopulated(self: *const Self) bool {
        const splitIndex = self.tx.?.db.getSplitIndex(self);
        if (splitIndex == -1) {
            return false;
        }
        return true;
    }

    fn serialize(self: *const Self, buf: []u8) void {
        var leftPos: usize = 0;
        var rightPos: usize = buf.len - 1;
        // Add page header: isLeaf, key-value pairs count, node num
        // isLeaf
        const _isLeaf = self.isLeaf();
        var bitSetVar: u64 = 0;
        if (_isLeaf) {
            bitSetVar = 1;
        }
        buf[leftPos] = @as(u8, @intCast(bitSetVar));
        leftPos += 1;

        // key-value pairs count
        std.mem.writeInt(u16, buf[leftPos..(leftPos + 2)], @as(u16, @intCast(self.items.items.len)), std.builtin.Endian.big);
        leftPos += 2;

        // We use slotted pages for storing data in the page. It means the actual keys and values (the cells) are appended
        // to right of the page whereas offsets have a fixed size and are appended from the left.
        // It's easier to preserve the logical order (alphabetical in the case of b-tree) using the metadata and performing
        // pointer arithmetic. Using the data itself is harder as it varies by size.

        // Page structure is:
        // ----------------------------------------------------------------------------------
        // |  Page  | key-value /  child node    key-value 		      |    key-value		 |
        // | Header |   offset /	 pointer	  offset         .... |      data      ..... |
        // ----------------------------------------------------------------------------------

        for (self.items.items, 0..) |item, i| {
            if (!_isLeaf) {
                const childNode = self.childNodes.items[i];
                // Write the child page as a fixed size of 8 bytes.
                std.mem.writeInt(u64, buf[leftPos..(leftPos + 8)], childNode, std.builtin.Endian.big);
                leftPos += 8;
            }

            const klen = item.key.len;
            const vlen = item.value.len;

            // write offset
            const offset = rightPos - klen - vlen - 2;
            std.mem.writeInt(u16, buf[leftPos..(leftPos + 2)], @as(u16, @intCast(offset)), std.builtin.Endian.big);
            leftPos += 2;

            rightPos -= vlen;
            @memcpy(buf[rightPos..], item.value);

            rightPos -= 1;
            buf[rightPos] = vlen;

            rightPos -= klen;
            @memcpy(buf[rightPos..], item.key);
            rightPos -= 1;
            buf[rightPos] = klen;
        }

        if (!_isLeaf) {
            // Write the last child node
            const lastChildNode = self.childNodes.getLast();
            // Write the child page as a fixed size of 8 bytes.
            std.mem.writeInt(u64, buf[leftPos..(leftPos + 8)], lastChildNode, std.builtin.Endian.big);
        }
    }

    fn deserialize(self: *const Self, buf: []u8) void {
        var leftPos = 0;
        // Read header
        const _isLeaf = buf[leftPos] == 1;

        leftPos += 1;
        const itemsCount = std.mem.readInt(u16, buf[leftPos..(leftPos + 2)], std.builtin.Endian.big);
        leftPos += 2;

        // Read body
        for (0..itemsCount) |_| {
            if (!_isLeaf) { // false
                const pageNum = std.mem.readInt(u64, buf[leftPos..(leftPos + 8)], std.builtin.Endian.big);
                leftPos += 8;
                // Add child node
                self.childNodes.append(pageNum) catch unreachable;
            }

            // Read offset
            const offset = std.mem.readInt(u16, buf[leftPos..(leftPos + 2)], std.builtin.Endian.big);
            leftPos += 2;

            const klen = buf[offset];
            offset += 1;
            const key = buf[offset..(offset + klen)];
            offset += klen;

            const vlen = buf[offset];
            offset += 1;
            const value = buf[offset..(offset + vlen)];
            offset += vlen;
            self.items.append(Item.init(key, value)) catch unreachable;
        }

        // TODO why?
        if (!_isLeaf) { // False
            // Read the last child node
            const pageNum = std.mem.readInt(u64, buf[leftPos..(leftPos + 8)], std.builtin.Endian.big);
            self.childNodes.append(pageNum) catch unreachable;
        }
    }

    /// Returns the size of a key-value-childNode triplet at a given index. If the node is a leaf, then the size
    /// of a key-value pair is returned. It's assumed i <= items.len.
    fn elementSize(self: *const Self, i: usize) usize {
        var size = 0;
        size += self.items[i].key.len;
        size += self.items[i].value.len;
        size += 8; // 8 is the page number size
        return size;
    }

    /// Returns the node's size in bytes.
    fn nodeSize(self: *const Self) usize {
        var size = 0;
        size += Self.nodeHeaderSize;
        for (0..self.items.len) |i| {
            size += self.elementSize(i);
        }
        // Add last page
        size += 8; // 8 is the page number size
        return size;
    }

    /// Searches for a key inside the tree. Once the key is found, the parent node and the correct index are returned
    /// so the key itself can be accessed in the following way parent[index]. A list of the node ancestors (not including the
    /// node itself) is also returned.
    /// If the key isn't found, we have 2 options. If exact is true, it means we expect findKey
    /// to find the key, so a falsey answer. If exact is false, then findKey is used to locate where a new key should be
    /// inserted so the position is returned.
    fn findKey(self: *const Self, key: []const u8, exact: bool) void {
        const ancestoreIndexes = std.ArrayList(usize).init(self.allocator);
        defer ancestoreIndexes.deinit();
    }

    fn findKeyHelper(self: *const Self, node: *const Node, key: []const u8, exact: bool, ancestoreIndexes: *std.ArrayList(usize)) void {}

    fn addItem(self: *Self, item: *Item, insertionIndex: usize) usize {
        self.items.insert(insertionIndex, item) catch unreachable;
        return insertionIndex;
    }

    fn merge(self: *Self, bNode: *Node, bNodeIndex: usize) !void {
        // 	               p                                     p
        //                3,5                                    5
        //	      /        |       \       ------>         /          \
        //       a   	   b        c                     a            c
        //     1,2         4        6,7                 1,2,3,4         6,7

        const aNode = try self.getNode(self.childNodes.items[bNodeIndex - 1]);

        // Take the item from the parent, remove it and add it to the unbalanced node

    }
};
