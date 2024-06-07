const std = @import("std");
const transaction = @import("./transaction.zig");
const compare = @import("./util.zig").compare;

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
    // associated transaction
    tx: *transaction.TX,

    pageNum: u64,
    items: std.ArrayList(*Item),
    childNodes: std.ArrayList(u64),

    allocator: ?std.mem.Allocator,
    const Self = @This();
    // u8[leaf flag], u16[item count]
    pub const nodeHeaderSize: usize = 3;

    // new a empty node with allocator
    pub fn init(allocator: std.mem.Allocator) *Self {
        var self: *Self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.childNodes = std.ArrayList(u64).init(allocator);
        self.items = std.ArrayList(*Item).init(allocator);
        self.pageNum = 0;
        self.tx = null;
    }

    /// creates a new node only with the properties that are relevant when savuing to the disk
    pub fn initNodeForSerialization(allocator: std.mem.Allocator, _items: std.ArrayList(*Item), _childNodes: std.ArrayList(u64)) *Self {
        var self: *Self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.childNodes = _childNodes;
        self.items = _items;
        self.pageNum = 0;
        self.tx = null;
        return self;
    }

    pub fn destroy(self: *Self) void {
        for (self.items.items) |item| {
            item.destroy();
        }
        self.items.deinit();
        self.childNodes.deinit();
        if (self.allocator) |allocator| {
            allocator.destroy(self);
        }
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
    pub fn writeNode(self: *Self, node: *Node) *Node {
        return self.tx.writeNode(node);
    }

    fn writeNodes(self: *Self, nodes: []*Node) void {
        for (nodes) |node| {
            _ = self.tx.writeNode(node);
        }
    }

    /// Get pageNum's node from the transaction
    fn getNode(self: *const Self, pageNum: u64) !*Node {
        return self.tx.getNode(pageNum);
    }

    /// Checks if the node size is bigger than the size of a page.
    pub fn isOverPopulated(self: *const Self) bool {
        return self.tx.db.dal.isOverPopulated(self);
    }

    // checks if the node size is big enough to populate a page after giving away one item.
    fn canSpareAnElement(self: *const Self) bool {
        return self.tx.db.dal.getSplitIndex(self) != null;
    }

    // Checks if the node size is smaller than the size of page.
    pub fn isUnderPopulated(self: *const Self) bool {
        return self.tx.db.dal.isUnderPopulated(self);
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

    pub fn deserialize(self: *const Self, buf: []u8) void {
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
        size += self.items.items[i].key.len;
        size += self.items.items[i].value.len;
        size += 8; // 8 is the page number size
        return size;
    }

    /// Returns the node's size in bytes.
    pub fn nodeSize(self: *const Self) usize {
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
    pub fn findKey(self: *const Self, key: []const u8, exact: bool) !std.meta.Tuple(&.{ ?usize, *Node, std.ArrayList(usize) }) {
        const ancestoreIndexes = std.ArrayList(usize).init(self.allocator);
        const find = try self.findKeyHelper(self, key, exact, &ancestoreIndexes);
        return &.{ find[0], find[1], ancestoreIndexes };
    }

    // Find a key from node
    fn findKeyHelper(node: *const Node, key: []const u8, exact: bool, ancestoreIndexes: *std.ArrayList(usize)) !std.meta.Tuple(&.{ ?usize, *Node }) {
        const find = node.findKeyInNode(key);
        const wasFound = find[0];
        const index = find[1];
        if (wasFound) {
            return &.{ index, node };
        }
        // If we reached a leaf node and the key was'nt found, it means it doesn't exist.
        if (node.isLeaf()) {
            if (exact) {
                return &.{ null, null };
            }

            return &.{ index, node };
        }

        // Else keep searching the tree
        try ancestoreIndexes.append(index);
        const nextChild = try node.getNode(node.childNodes.items[index]);
        return Self.findKeyHelper(nextChild, key, exact, ancestoreIndexes);
    }

    // Iterates all the items and finds the key. If the key is found, then the item is returned. If the key
    // isn't found then return the index where it should have been (the first index that key is greater than it's previous)
    fn findKeyInNode(self: *const Self, key: []const u8) std.meta.Tuple(&.{ bool, usize }) {
        for (self.items.items, 0..) |existingItem, i| {
            switch (compare({}, existingItem.key, key)) {
                .eq => {
                    // key match
                    return .{ true, i };
                },
                .gt => {
                    // The key is bigger than the previous item, so it doesn't exist in the node, but may exist in child nodes.
                    return .{ false, i };
                },
                .lt => {},
            }
        }

        // The key isn't bigger than any of the items which means it's in the last index.
        return .{ false, self.items.items.len };
    }
    pub fn addItem(self: *Self, item: *Item, insertionIndex: usize) usize {
        self.items.insert(insertionIndex, item) catch unreachable;
        return insertionIndex;
    }

    // split rebalances the tree after adding. After insertion the modified node has to be checked to make sure it
    // didn't exceed the maximum number of elements. If it did, then it has to be split and rebalanced. The transformation
    // is depicted in the graph below. If it's not a leaf node, then the children has to be moved as well as shown.
    // This may leave the parent unbalanced by having too many items so rebalancing has to be checked for all the ancestors.
    // The split is performed in a for loop to support splitting a node more than once. (Though in practice used only once).
    // 	           n                                        n
    //                 3                                       3,6
    //	      /        \           ------>       /          |          \
    //	   a           modifiedNode            a       modifiedNode     newNode
    //   1,2                 4,5,6,7,8            1,2          4,5         7,8
    pub fn split(self: *Self, nodeToSplit: *Node, nodeToSplitIndex: usize) !void {
        // The first index where min amount of bytes to populate a page is achieved. Then add 1 so it will be split one
        // index after.
        const splitIndex = nodeToSplit.tx.db.dal.getSplitIndex(nodeToSplit) orelse return error.Santy;
        const middItem = nodeToSplit.items.items[splitIndex];
        var newNode: *Node = null;
        if (nodeToSplit.isLeaf()) {
            const tmpNode = self.tx.newNode(nodeToSplit.items.items[splitIndex + 1 ..], &[_]u64{});
            newNode = self.writeNode(tmpNode);
            try nodeToSplit.items.resize(splitIndex);
        } else {
            const tmpNode = self.tx.newNode(nodeToSplit.items.items[splitIndex + 1 ..], nodeToSplit.childNodes.items[splitIndex + 1 ..]);
            newNode = self.writeNode(tmpNode);
            try nodeToSplit.items.resize(splitIndex);
            try nodeToSplit.childNodes.resize(splitIndex + 1);
        }

        _ = self.addItem(middItem, nodeToSplit);
        try self.childNodes.insert(nodeToSplitIndex + 1, newNode.pageNum);
        self.writeNodes([_]*Node{ self, nodeToSplit });
    }

    // rebalanceRemove rebalances the tree after a remove operation. This can be either by rotating to the right, to the
    // left or by merging. First, the sibling nodes are checked to see if they have enough items for rebalancing
    // (>= minItems+1). If they don't have enough items, then merging with one of the sibling nodes occurs. This may leave
    // the parent unbalanced by having too little items so rebalancing has to be checked for all the ancestors.
    pub fn rebalanceRemove(self: *Self, unbalanceNode: *Node, unbalanceIndex: usize) !void {
        const pNode = self;
        // Right rotate
        if (unbalanceIndex != 0) {
            const leftNode = try self.getNode(pNode.childNodes.items[unbalanceIndex - 1]);
            if (leftNode.canSpareAnElement()) {
                Self.rotateRight(leftNode, pNode, unbalanceNode, unbalanceIndex);
                self.writeNodes([]*Node{ leftNode, pNode, unbalanceNode });
                return;
            }
        }

        // Left rotate
        if (unbalanceIndex != (pNode.items.len - 1)) {
            const rightNode = try self.getNode(pNode.childNodes.items[unbalanceIndex + 1]);
            if (rightNode.canSpareAnElement()) {
                Self.rotateLeft(unbalanceNode, pNode, rightNode, unbalanceIndex);
                self.writeNodes([]*Node{ unbalanceNode, pNode, rightNode });
                return;
            }
        }

        // The merge function merges a given node with its node to the right. So by default, we merge an unbalanced node
        // with its right sibling. In the case where the unbalanced node is the leftmost, we have to replace the merge
        // parameters, so the unbalanced node right sibling, will be merged into the unbalanced node.
        if (unbalanceIndex == 0) {
            const rightNode = try self.getNode(pNode.childNodes.items[unbalanceIndex + 1]);
            return Self.merge(rightNode, unbalanceIndex + 1);
        }

        return pNode.merge(unbalanceNode, unbalanceIndex);
    }

    // Removes an item from a leaf node. it means there is no handling of child nodes.
    pub fn removeItemFromLeaf(self: *Self, index: usize) void {
        _ = self.items.orderedRemove(index);
        self.writeNode(self);
    }

    // https://miro.medium.com/v2/resize:fit:1400/format:webp/1*U44xKwRNxHWt2npMz7hU7A.png
    pub fn removeItemFromInternal(self: *Self, index: usize) ![]usize {
        // Take element before inorder (The biggest element from the left branch), put it in the removed index and remove
        // it from the original node. Track in affectedNodes any nodes in the path leading to that node. It will be used
        // in case the tree needs to be rebalanced.
        //          p
        //       /
        //     ..
        //  /     \
        // ..      a
        const affectedNodes = std.ArrayList(usize).init(self.allocator);
        try affectedNodes.append(index);

        // Starting from its left child, descend to the rightmost descendant.
        var aNode = try self.getNode(self.childNodes.items[index]);
        while (!aNode.isLeaf()) {
            const travesingIndex = self.childNodes.items.len - 1;
            aNode = try aNode.getNode(aNode.childNodes.items[travesingIndex]);
            try affectedNodes.append(travesingIndex);
        }

        // Replace the item that should be removed with the item before inorder which we just found.
        self.items.items[index] = aNode.items.pop() catch unreachable;
        self.writeNodes([_]*Node{ self, aNode });

        const _affectedNodes = try affectedNodes.toOwnedSlice();
        return _affectedNodes;
    }

    fn rotateRight(aNode: *Node, pNode: *Node, bNode: *Node, bNodeIndex: usize) void {
        // 	           p                                    p
        //             4                                    3
        //	      /        \           ------>         /          \
        //	   a           b (unbalanced)            a        b (unbalanced)
        //   1,2,3             5                     1,2            4,5

        // Get last item and remove it
        const aNodeItem = aNode.items.pop(); // 3, if 3 is branch, it's last children also should be move

        // Get item from parent node and assign the aNodeItem item instead
        const pNodeItemIndex: usize = if (Self.isFirst(bNodeIndex)) 0 else bNodeIndex;
        const pNodeItem = pNode.items.items[pNodeItemIndex]; // 4
        pNode.items.items[pNodeItemIndex] = aNodeItem; // p4->p3

        // Assign parent item to b and make it first
        bNode.items.insert(0, pNodeItem) catch unreachable; // 4 --> b level, as the first item

        // If it's an inner leaf then move children as well.
        if (!aNode.isLeaf()) {
            const childNodeToShift = aNode.childNodes.pop(); // a' last one children move to b level
            bNode.childNodes.insert(0, childNodeToShift); // move 3's children to b level as 4's left children
        }
    }

    fn rotateLeft(aNode: *Node, pNode: *Node, bNode: *Node, bNodeIndex: usize) void {
        // 	           p                                     p
        //             2                                     3
        //	      /        \           ------>         /          \
        //  a(unbalanced)    b                 a(unbalanced)        b
        //   1                3,4,5                   1,2             4,5

        // Get first item and remove it
        const bNodeItem = bNode.items.orderedRemove(0);

        // Get item from parent node and assign the bNodeItem item instead
        const pNodeItemIndex: usize = bNodeIndex;
        if (Self.isLast(pNode, bNodeIndex)) {
            // Why need to check if it's the last item
            pNodeItemIndex = pNode.items.items.len - 1;
        }
        const pNodeItem = pNode.items.items[pNodeItemIndex];
        pNode.items.items[pNodeItemIndex] = bNodeItem;
        // Assign parent item to a and make it last
        aNode.items.append(pNodeItem) catch unreachable;

        // If it's an inner leaf then move children as well.
        if (!bNode.isLeaf()) {
            const childNodeToShift = bNode.childNodes.orderedRemove(0);
            aNode.childNodes.append(childNodeToShift) catch unreachable;
        }
    }

    fn merge(self: *Self, bNode: *Node, bNodeIndex: usize) !void {
        // 	               p                                     p
        //                3,5                                    5
        //	      /        |       \       ------>         /          \
        //       a   	   b        c                     a            c
        //     1,2         4        6,7                 1,2,3,4         6,7

        const aNode = try self.getNode(self.childNodes.items[bNodeIndex - 1]);

        // Take the item from the parent, remove it and add it to the unbalanced node
        const pNodeItem = self.items.items[bNodeIndex - 1];
        _ = self.items.orderedRemove(bNodeIndex);

        try aNode.items.append(pNodeItem);
        _ = self.childNodes.orderedRemove(bNodeIndex);

        if (!aNode.isLeaf()) {
            try aNode.childNodes.appendSlice(bNode.childNodes.items);
        }

        self.writeNodes([_]*Node{aNode, self});
        self.tx.deleteNode(bNode); // recycel the node
    }
};
