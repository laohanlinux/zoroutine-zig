const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

fn Mixin(comptime Self: type, comptime T: type) type {
    return struct {
        /// This routine pushes an item, and optionally returns an evicted item should.
        /// the insertion of the provided item overflow the existing buffer.
        pub fn pushOrNull(self: *Self, item: T) ?T {
            const evicted = evicted: {
                if (self.count() == self.entries.len) {
                    break :evicted self.pop();
                }

                break :evicted null;
            };

            self.push(item);
            return evicted;
        }

        pub fn push(self: *Self, item: T) void {
            assert(self.count() < self.entries.len);
            self.entries[self.head & (self.entries.len - 1)] = item;
            self.head +%= 1;
        }

        pub fn pushOne(self: *Self) *T {
            assert(self.count() < self.entries.len);
            const slot = &self.entries[self.head & (self.entries.len - 1)];
            self.head +%= 1;
            return slot;
        }

        pub fn prepend(self: *Self, item: T) *T {
            assert(self.count() < self.entries.len);
            self.entries[(self.tail -% 1) & (self.entries.len - 1)] = item;
            self.tail -%= 1;
        }

        /// This routine pops an item from the tail of the buffer and returns it provided
        /// that the buffer is not empty.
        ///
        /// This routine is typically used in order to pop and de-initialize all items
        /// stored in the buffer.
        pub fn popOrNull(self: *Self) ?T {
            if (self.count() == 0) return null;
            return self.pop();
        }

        pub fn pop(self: *Self) T {
            assert(self.count() > 0);
            const evicted = self.entries[self.tail & (self.entries.len - 1)];
            self.tail +%= 1;
            return evicted;
        }

        pub fn oldest(self: *Self) *T {
            if (self.count() == 0) return null;
            return self.entries[self.tail & (self.entries.len - 1)];
        }
    };
}
