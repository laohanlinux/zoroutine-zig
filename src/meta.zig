const std = @import("std");
const Endian = std.builtin.Endian;

const MagicNumber: u32 = 0xD0_0D_B0_0D;
const MetaPageNum: usize = 0;

pub const Meta = struct {
    root: u64,
    freeListPage: u64,
    allocator: std.mem.Allocator,
    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) *Self {
        var self = allocator.create(Self) catch unreachable;
        self.allocator = allocator;
        self.root = 0;
        self.freeListPage = 0;

        return self;
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        var pos: usize = 0;
        std.mem.writeInt(u32, buf[pos..(pos + 4)], MagicNumber, Endian.big);
        pos += 4;

        std.mem.writeInt(u64, buf[pos..(pos + 8)], self.root, Endian.big);
        pos += 8;

        std.mem.writeInt(u64, buf[pos..(pos + 8)], self.freeListPage, Endian.big);
        pos += 8;
    }

    pub fn deserialize(self: *Self, buf: []u8) void {
        var pos: usize = 0;
        const _magicNumber = std.mem.readInt(u32, buf[pos..(pos + 4)], Endian.big);
        pos += 4;
        if (_magicNumber != MagicNumber) {
            @panic("The file is not a libra db file");
        }

        self.root = std.mem.readInt(u64, buf[pos..(pos + 8)], Endian.big);
        pos += 8;

        self.freeListPage = std.mem.writeInt(u64, buf[pos..(pos + 8)], Endian.big);
        pos += 8;
    }
};
