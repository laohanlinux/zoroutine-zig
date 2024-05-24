const std = @import("std");
const testing = std.testing;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

fn div(a: usize, b: usize) ?usize {
    _ =  a + b;
    return null;
}

test "basic add functionality" {
    const ok = div(0, 0);
    std.debug.print("{?}\n", .{ok == null});
    // try testing.expect(div(3, 7) == 10);
}
