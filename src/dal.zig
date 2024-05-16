const std = @import("std");

pub const Option = struct {};

pub const Dal = struct {
    pageSize: usize,
    minFillPercent: f32,
    maxFillPercent: f32,

    file: *std.fs.File,
};
