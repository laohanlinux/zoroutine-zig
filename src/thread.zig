const std = @import("std");
const Thread = std.Thread;
const Semaphore = Thread.Semaphore;
const Value = std.atomic.Value;

/// A mutex that can be shared between threads.
pub const Mutex = struct {
    sem: Semaphore,

    /// Initialize the mutex.
    pub fn init() Mutex {
        return .{ .sem = Semaphore{ .permits = 1 } };
    }

    /// Lock the mutex.
    pub fn lock(self: *@This()) void {
        self.sem.wait();
    }

    /// Unlock the mutex.
    pub fn unlock(self: *@This()) void {
        self.sem.post();
    }

    /// Try to lock the mutex.
    pub fn tryLock(self: *@This(), timeout_ns: usize) error{Timeout}!void {
        return self.sem.timedWait(timeout_ns);
    }
};

/// A mutex that allows multiple readers but only one writer.
/// This is a reader-writer mutex implementation using semaphores and atomic operations.
/// TODO: add a timeout for the mutex and add a priority for the writer or reader,
/// avoid writer, reader starvation.
pub const RxMutex = struct {
    write_sem: Semaphore,
    read_sem: Semaphore,
    readers: Value(i64),
    writer: Value(bool),

    /// Initialize the mutex.
    pub fn init() RxMutex {
        return .{ .write_sem = Semaphore{ .permits = 1 }, .read_sem = Semaphore{ .permits = 1 }, .readers = Value(i64).init(0), .writer = Value(bool).init(false) };
    }

    /// Lock the mutex for reading.
    pub fn lockShared(self: *@This()) void {
        // wait for a reader to release the lock
        self.read_sem.wait();
        // wait for the writer to release the lock
        while (self.writer.load(.acquire)) {
            // release the read lock, and wait for the writer to release the lock
            self.read_sem.post();
            // avoid busy-waiting
            _ = Thread.yield() catch unreachable;
            // wait for a reader to release the lock again
            self.read_sem.wait();
        }
        // get the read lock.
        const readers = self.readers.fetchAdd(1, .release);
        if (readers == 0) {
            // the first reader, wait for the writer to release the lock
            // this is to ensure that the writer is not blocked by readers
            // post the write sem when the last reader releases the lock
            self.write_sem.wait();
        }
        // release the read lock, so other readers can get the lock
        self.read_sem.post();
    }

    /// Unlock the mutex for reading.
    pub fn unlockShared(self: *@This()) void {
        const readers = self.readers.fetchSub(1, .release);
        if (readers <= 0) {
            @panic("RxMutex: Unbalanced unlockShared");
        }
        if (readers == 1) {
            // the last reader, post the write sem
            self.write_sem.post(); // Allow the writer to acquire the lock
        }
    }

    /// Lock the mutex for writing.
    pub fn lockExclusive(self: *@This()) void {
        self.write_sem.wait(); // Wait for the writer to release the lock
        self.writer.store(true, .release); // Set the writer flag
    }

    /// Unlock the mutex for writing.
    pub fn unlockExclusive(self: *@This()) void {
        // Before unlockExclusive, the writer must be true, the writer's owner must be not empty
        if (self.writer.cmpxchgStrong(true, false, .release, .acquire)) |_| {
            @panic("RxMutex: Unbalanced unlockExclusive"); //
        }
        self.write_sem.post(); // Allow other readers to acquire the lock
    }
};

test "ShareMutex" {
    var mutex = Mutex.init();
    mutex.lock();
    mutex.unlock();

    _ = Thread.spawn(.{}, struct {
        fn run(ctx: *Mutex) void {
            ctx.lock();
            std.time.sleep(std.time.ns_per_s * 3);
            ctx.unlock();
        }
    }.run, .{&mutex}) catch unreachable;

    mutex.tryLock(std.time.ns_per_s) catch |err| {
        std.debug.assert(err == error.Timeout);
    };

    std.time.sleep(std.time.ns_per_s * 5);
    mutex.unlock();
}

test "RxMutex" {
    var mutex = RxMutex.init();
    mutex.lockExclusive();
    mutex.unlockExclusive();

    for (0..100) |_| {
        mutex.lockShared();
    }
    for (0..100) |_| {
        mutex.unlockShared();
    }

    mutex.lockExclusive();
    _ = Thread.spawn(.{}, struct {
        fn run(ctx: *RxMutex) void {
            ctx.unlockExclusive();
        }
    }.run, .{&mutex}) catch unreachable;
    std.time.sleep(std.time.ns_per_s);
}
