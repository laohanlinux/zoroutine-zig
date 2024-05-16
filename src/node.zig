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
    children: std.ArrayList(u8),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) *Self {
        var self: *Self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.children = std.ArrayList(u64).init(allocator);
        self.items = std.ArrayList(*Item).init(allocator);
        self.tx = null;
    }

    pub fn destroy(self: *Self) void {
        for (self.items.items) |item| {
            item.destroy();
        }
        self.items.deinit();
        self.children.deinit();
        self.allocator.destroy(self);
    }

    pub fn isLeaf(self: *const Self) bool {
        return (self.children.items.len == 0);
    }

    pub fn isLast(parentNode: *const Node, index: usize) bool {
        return (parentNode.items.items.len == index);
    }

    pub fn isFirst(index: usize) bool {
        return (index == 0);
    }

    fn writeNode(self: *Self, nodes: []*Node) void {
        for (nodes) |node| {
            self.tx.?.writeNode(node);
        }
    }
};
