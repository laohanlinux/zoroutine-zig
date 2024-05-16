const std = @import("std");
const node = @import("./node.zig");
const Node = node.Node;
pub const TX = struct {
    dirtyNodes: std.HashMap(u64, *Node, {}, 80),
    pagesToDelete: std.ArrayList(u64),

    // new pages allocated during the transaction. They will be released if rollback is called.
    allocatedPageNums: std.ArrayList(u64),
    write: bool,
    db: *DB,
};
