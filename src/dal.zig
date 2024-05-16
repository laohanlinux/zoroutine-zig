const std = @import("std");
const Meta = @import("./meta.zig").Meta;
const FreeList = @import("./freelist.zig").FreeList;

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
    num: u64,
    data: []u8,
};

pub const Dal = struct {
    pageSize usize,
    minFillPercent: f32,
    maxFillPercent: f32,
    file: *std.fs.File,
    meta: *Meta,
    freelist: *FreeList,
    allocator: std.mem.Allocator,

    const Self = @This();
    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: Options) !Self {
        var dal: Dal = undefined;
        dal.pageSize = options.pageSize;

        return dal;
    }
};
