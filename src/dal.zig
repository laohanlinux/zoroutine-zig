const std = @import("std");
const Meta = @import("./meta.zig").Meta;
const FreeList = @import("./freelist.zig").FreeList;
const Node = @import("./node.zig").Node;

pub const Options = struct {
    pageSize: usize,
    minFillPercent: f32,
    maxFillPercent: f32,
};

pub const DefaultOptions = Options{
    .pageSize = 0,
    .minFillPercent = 0.5,
    .maxFillPercent = 0.9,
};

const Page = struct {
    num: u64, // page id
    data: []u8,

    pub fn init(allocator: std.mem.Allocator, dataSize: usize) !*Page {
        const page = try allocator.create(Page);
        page.*.num = 0;
        page.*.data = try allocator.alloc(u8, dataSize);
        @memset(page.data, 0);
        return page;
    }

    pub fn deinit(self: *Page, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        allocator.destroy(self);
    }
};

pub const Dal = struct {
    pageSize: usize,
    minFillPercent: f32,
    maxFillPercent: f32,
    file: std.fs.File,
    meta: *Meta,
    freelist: *FreeList,
    allocator: std.mem.Allocator,

    const Self = @This();
    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: Options) !*Self {
        var dal = try allocator.create(Self);
        dal.pageSize = options.pageSize;
        dal.allocator = allocator;
        const stat = std.fs.cwd().statFile(path) catch |err| {
            switch (err) {
                std.fs.File.OpenError.FileNotFound => {
                    dal.file = try std.fs.cwd().createFile(path, std.fs.File.CreateFlags{ .exclusive = true });
                    dal.meta = try allocator.create(Meta);
                    dal.freelist = try allocator.create(FreeList);
                    dal.meta.freeListPage = dal.freelist.getNextPage();
                    _ = try dal.writeFreelist();

                    // init root
                    const collectionsMode = try dal.writeNode(try Node.init(allocator));
                    dal.root = collectionsMode.pageNum;

                    // Write meta page.
                    _ = try dal.writeNode(dal.meta); // other error
                    return dal;
                },
                else => {
                    return err;
                },
            }
        };
        std.log.info("the db has exist, fileSize: {d}", .{stat.size});
        dal.file = try std.fs.cwd().openFile(path, std.fs.File.OpenFlags{ .mode = .read_write });
        dal.meta = try dal.readMeta();
        dal.freelist = try dal.readFreelist();
        return dal;
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
        self.allocator.destroy(self.meta);
        self.allocator.destroy(self.freelist);
        self.allocator.destroy(self);
    }

    // Should be called when performing rebalance after an item is removed. It checks if a node can spare an
    // element, and if it does then it returns the index when there the split should happen. Otherwise None is returned.
    pub fn getSplitIndex(self: *const Self, node: *const Node) ?usize {
        var size: usize = 0;
        size += Node.nodeHeaderSize;
        for (node.items.items, 0..) |_, i| {
            size += node.elementSize(i);
            // if we have a big enough page size (more than minimum), and didn't reach the last node, which means we can
            // spare an element
            if (@as(f32, @intCast(size)) > self.maxThreshold() and i < node.items.items.len - 1) {
                return i + 1; // NOTE: return the next index
            }
        }

        return null;
    }

    // For split
    pub fn maxThreshold(self: *const Self) f32 {
        return self.maxFillPercent * @as(f32, @intCast(self.pageSize));
    }

    pub fn isOverPopulated(self: *const Self, node: *const Node) bool {
        return @as(f32, @intCast(node.nodeSize())) > self.maxThreshold();
    }

    // For merge
    pub fn minThreshold(self: *const Self) f32 {
        return self.minFillPercent * @as(f32, @intCast(self.pageSize));
    }

    pub fn isUnderPopulated(self: *Self) bool {
        return self.freelist.?.isUnderPopulated();
    }

    fn allocateEmptyPage(self: *Self) !*Page {
        const page = try Page.init(self.allocator, self.pageSize);
        return page;
    }

    // read a page from disk file.
    fn readPage(self: *Self, pageNum: u64) !*Page {
        const page = try self.allocateEmptyPage();
        const offset: usize = pageNum * @as(u64, self.pageSize);
        _ = try self.file.pread(page.data, offset);
        return page;
    }

    // write a page to disk file
    fn writePage(self: *Self, page: *Page) !void {
        const offset: u64 = page.*.num * @as(u64, self.pageSize);
        try self.file.pwriteAll(page.data, offset);
    }

    // disk -[copy]-> page -[ref(key,value)]-> node
    fn getNode(self: *Self, pageNum: u64) !*Node {
        const page = try self.readPage(pageNum);
        var node = Node.init(self.allocator);
        node.deserialize(page.data);
        node.*.pageNum = pageNum;
        return node;
    }

    // node -copy-> page -copy-> disk
    fn writeNode(self: *Self, node: *Node) !void {
        const page = try self.allocateEmptyPage();
        if (node.*.pageNum == 0) { // TODO Why
            page.*.num = self.freelist.getNextPage();
            node.*.pageNum = page.*.num;
        } else {
            page.*.num = node.*.pageNum;
        }
        node.serialize(page.data);
        try self.writePage(page);
    }

    // delete node and release it to freelist
    fn deleteNode(self: *Self, pageNum: u64) void {
        self.freelist.releasePage(pageNum);
    }

    // load freelist page
    fn readFreelist(self: *Self) !*FreeList {
        const page = try self.readPage(self.meta.freeListPage);
        var freelist = FreeList.init(self.allocator);
        freelist.deserialize(page.data);
        return freelist;
    }

    // write freelist to page
    fn writeFreelist(self: *Self) !*Page {
        const page = try self.allocateEmptyPage();
        page.*.num = self.meta.*.freeListPage;
        self.freelist.serialize(page.data);
        try self.writePage(page);
        return page;
    }

    fn writeMeta(self: *Self, meta: *Meta) !*Page {
        const page = try self.allocateEmptyPage();
        page.*.num = Meta.pageMetaNum;
        meta.serialize(page.data);
        try self.writePage(page);
        return page;
    }

    fn readMeta(self: *Self) !*Meta {
        const page = try self.readPage(Meta.pageMetaNum);
        const meta = Meta.init(self.allocator);
        meta.deserialize(page.data);
        return meta;
    }
};

test "dal" {
    const fileName = generateTmpFile("dirty_file");
    var dal = try Dal.init(std.testing.allocator, fileName, DefaultOptions);
    defer dal.deinit();
    std.debug.print("{any}\n", .{dal});
}

fn generateTmpFile(suffix: []const u8) []const u8 {
    const tmpDir = std.testing.tmpDir(.{});
    // std.log.info("{s}", tmpDir.dir.metadata());
    const relativePath = std.fs.path.join(std.heap.page_allocator, &.{ "zig-cache", "tmp", tmpDir.sub_path[0..] }) catch unreachable;
    defer std.heap.page_allocator.free(relativePath);
    const baseDir = std.fs.realpathAlloc(std.heap.page_allocator, relativePath) catch unreachable;
    defer std.heap.page_allocator.free(baseDir);
    _ = std.fs.cwd().makeDir(baseDir) catch {};
    const fileName = fprint(std.heap.page_allocator, "{any}{s}", .{ std.time.nanoTimestamp(), suffix });
    defer std.heap.page_allocator.free(fileName);

    const paths = &[_][]const u8{
        baseDir,
        fileName,
    };
    const fullFileName = std.fs.path.join(std.heap.page_allocator, paths) catch unreachable;
    // _ = std.fs.cwd().createFile(fullFileName, .{ .read = true }) catch unreachable;
    std.log.info("the badger path: {s}", .{fullFileName});
    return fullFileName;
}

pub fn fprint(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) []const u8 {
    const buffer = std.fmt.allocPrint(allocator, fmt, args) catch unreachable;
    return buffer;
}
