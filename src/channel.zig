const std = @import("std");
const Thread = std.Thread;
/// A channel that can be used to send and receive values between threads.
pub fn Channel(comptime T: type) type {
    return struct {
        mutex: Thread.Mutex,
        not_empty: Thread.Condition,
        not_full: Thread.Condition,
        buffer: std.ArrayList(T),
        capacity: usize,
        closed: bool,

        const Self = @This();

        /// Initialize a new channel with the given capacity.
        pub fn init(allocator: std.mem.Allocator, capacity: usize) Self {
            return .{
                .mutex = .{},
                .not_empty = .{},
                .not_full = .{},
                .buffer = std.ArrayList(T).init(allocator),
                .capacity = capacity,
                .closed = false,
            };
        }

        /// Send a value to the channel.
        pub fn send(self: *Self, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) {
                return error.ChannelClosed;
            }

            // wait until the there's space in the buffer.
            while (self.buffer.items.len >= self.capacity) {
                self.not_full.wait(&self.mutex);
                if (self.closed) {
                    return error.ChannelClosed;
                }
                try Thread.yield();
            }

            // add the value to the buffer.
            try self.buffer.append(value);
            self.not_empty.signal();
        }

        /// Try to send a value to the channel.
        pub fn trySend(self: *Self, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) {
                return error.ChannelClosed;
            }

            // If there's space in the buffer, add the value.
            if (self.buffer.items.len >= self.capacity) {
                return error.ChannelFull;
            }

            // Add the value to the buffer.
            try self.buffer.append(value);
            self.not_empty.signal();
        }

        /// Receive a value from the channel.
        pub fn recv(self: *Self) !T {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Wait until there's data to receive.
            while (self.buffer.items.len == 0) {
                if (self.closed) {
                    return error.ChannelClosed;
                }
                self.not_empty.wait(&self.mutex);
                try std.Thread.yield();
            }

            const value = self.buffer.orderedRemove(0);
            self.not_full.signal();
            return value;
        }

        /// Try to receive a value from the channel.
        pub fn tryRecv(self: *Self) !T {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) {
                return error.ChannelClosed;
            }

            // Check if channel is empty.
            if (self.buffer.items.len == 0) {
                return error.ChannelEmpty;
            }

            // Remove the value from the buffer.
            const value = self.buffer.orderedRemove(0);
            self.not_full.signal();
            return value;
        }

        /// Close the channel.
        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.closed) {
                self.closed = true;
                self.not_empty.broadcast();
                self.not_full.broadcast();
            }
        }

        /// Deinitialize the channel.
        pub fn deinit(self: *Self) void {
            self.buffer.deinit();
        }
    };
}

test "Channel" {
    var channel = Channel(usize).init(std.testing.allocator, 5);
    defer channel.deinit();

    try channel.send(1);
    try channel.send(2);
    try channel.send(3);
    try channel.send(4);
    try channel.send(5);
    channel.trySend(6) catch |err| {
        std.debug.assert(err == error.ChannelFull);
    };

    for (0..5) |i| {
        const received = try channel.tryRecv();
        std.debug.assert(received == i + 1);
    }

    _ = channel.tryRecv() catch |err| {
        std.debug.assert(err == error.ChannelEmpty);
    };

    // mutil send
    for (0..10) |_| {
        _ = try std.Thread.spawn(.{}, struct {
            fn run(_channel: *Channel(usize)) void {
                _channel.send(0) catch unreachable;
            }
        }.run, .{&channel});
    }

    std.time.sleep(std.time.ns_per_s * 1);
    for (0..10) |_| {
        const value = try channel.recv();
        std.debug.assert(value == 0);
    }
    channel.close();
    _ = channel.recv() catch |err| {
        std.debug.assert(err == error.ChannelClosed);
    };
}
