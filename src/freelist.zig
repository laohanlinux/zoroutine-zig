const std = @import("std");

/// metaPage is the maximum pgnum that is used by the db for its own purposes.
/// For now, only page 0 is used as the header page. It means all other numbers
///  can be used.
const metaPage = 0;

/// Freelist manages the manges free and used pages.
pub const FreeList = struct {
    /// maxPage holds the latest page num allocated.
    maxPage: u64,

    /// releasdPages holds all the ids that were released during delete. New page ids are first
    /// given from the releasedPageIDs to avoid growing the file. If it's empty, then maxPage is
    /// incremented and a new page is created thus increasing the file size.
    releasedPages: std.ArrayList(u64),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) *Self {
        return allocator.create(Self{
            .maxPage = 0,
            .releasedPages = std.ArrayList(u64).init(allocator),
            .allocator = allocator,
        }) catch unreachable;
    }

    pub fn destroy(self: *Self, allocator: std.mem.Allocator) void {
        self.releasedPages.deinit();
        allocator.destroy(self);
    }

    /// Returns page ids for writing New page ids are first given from the releasedPageIDs to avoid growing the file.
    /// If it's empty, then maxPage is incremented and a new page is created thus increasing the file size.
    pub fn getNextPage(self: *Self) u64 {
        if (self.releasedPages.popOrNull()) |page| {
            return page;
        } else {
            self.maxPage += 1;
            return self.maxPage;
        }
    }

    pub fn releasePage(self: *Self, page: u64) void {
        self.releasedPages.append(page) catch unreachable;
    }

    /// serialize serializes the freelist to a buffer.
    pub fn serialize(self: *Self, buf: []u8) void {
        var pos: usize = 0;
        std.mem.writeInt(u16, buf[pos..(pos + 2)][0..2], @as(u16, @intCast(self.maxPage)), std.builtin.Endian.big);
        pos += 2;

        // released pages count
        std.mem.writeInt(u16, buf[pos..(pos + 2)][0..2], @as(u16, @intCast(self.releasedPages.items.len)), std.builtin.Endian.big);
        pos += 2;

        for (self.releasedPages.items) |page| {
            std.mem.writeInt(u64, buf[pos..(pos + 8)][0..8], page, std.builtin.Endian.big);
            pos += 8;
        }
        return;
    }

    /// deserialize deserializes the freelist from a buffer.
    pub fn deserialize(self: *Self, buf: []u8) void {
        var pos: usize = 0;
        const _maxPage = std.mem.readInt(u16, buf[pos..(pos + 2)], std.builtin.Endian.big);
        self.maxPage = @as(u16, _maxPage);
        pos += 2;

        // released pages count
        const releasePageCount = std.mem.readInt(u16, buf[pos..(pos + 2)], std.builtin.Endian.big);
        pos += 2;

        // released pages
        while (releasePageCount > 0) : (releasePageCount -= 1) {
            self.releasedPages.append(std.mem.readInt(u64, buf[pos..(pos + 8)], std.builtin.Endian.big)) catch unreachable;
            pos += 8;
        }
    }
};
