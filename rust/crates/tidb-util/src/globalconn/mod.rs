// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete transcreation of Go `pkg/util/globalconn` (`globalconn.go` +
//! `pool.go`): cluster-unique connection IDs for the Global Kill feature.
//!
//! See `docs/design/2020-06-01-global-kill.md`. A GCID packs `serverID` and a
//! local connection ID with a markup bit selecting the 32-bit or 64-bit
//! layout; allocators hand out local IDs from an auto-increment pool (64-bit
//! side) or a lock-free circular pool (32-bit side), upgrading/downgrading
//! between widths as the 32-bit space fills and drains.
//!
//! Go's `logutil.BgLogger()` side-effect log lines map to `tracing`
//! macros; `sync2.AtomicInt32` (the is-64-bits flag) is an [`AtomicBool`].
//! Go's ldflag-injected test widths (`ldflagServerIDBits32` etc., compiled in
//! only for the global-kill integration-test build) are build machinery, not
//! runtime behavior, so the widths are constants here.

mod pool;

pub use pool::{AutoIncPool, IdPool, LockFreeCircularPool, ID_POOL_INVALID_VALUE};

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

/// Number of serverID bits in a 32-bit GCID.
pub const SERVER_ID_BITS32: u32 = 11;
/// Maximum serverID for a 32-bit GCID.
pub const MAX_SERVER_ID32: u64 = (1 << SERVER_ID_BITS32) - 1;
/// Number of local-connID bits in a 32-bit GCID.
pub const LOCAL_CONN_ID_BITS32: u32 = 20;
/// Maximum local connID for a 32-bit GCID.
pub const MAX_LOCAL_CONN_ID32: u64 = (1 << LOCAL_CONN_ID_BITS32) - 1;

/// Maximum serverID for a 64-bit GCID.
pub const MAX_SERVER_ID64: u64 = (1 << 22) - 1;
/// Number of local-connID bits in a 64-bit GCID.
pub const LOCAL_CONN_ID_BITS64: u32 = 40;
/// Maximum local connID for a 64-bit GCID.
pub const MAX_LOCAL_CONN_ID64: u64 = (1 << LOCAL_CONN_ID_BITS64) - 1;

/// Count of reserved connection IDs for internal processes.
pub const RESERVED_COUNT: u64 = 200;

/// The try count of 64-bit local connID allocation.
pub const LOCAL_CONN_ID_ALLOCATOR64_TRY_COUNT: usize = 10;

/// The Global Connection ID (Go `GCID`).
///
/// 32-bit layout: `|serverID(11b)|localConnID(20b)|markup=0|`;
/// 64-bit layout: `|0|serverID(22b)|localConnID(40b)|markup=1|`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Gcid {
    /// The server ID.
    pub server_id: u64,
    /// The server-local connection ID.
    pub local_conn_id: u64,
    /// Whether the 64-bit layout is used.
    pub is_64bits: bool,
}

impl Gcid {
    /// Returns the packed 64-bit connection ID (Go `ToConnID`).
    ///
    /// # Panics
    ///
    /// Panics when a field exceeds its layout width, with the source's
    /// messages.
    #[must_use]
    pub fn to_conn_id(&self) -> u64 {
        let mut id: u64 = 0;
        if self.is_64bits {
            assert!(
                self.local_conn_id <= MAX_LOCAL_CONN_ID64,
                "unexpected localConnID {} exceeds {}",
                self.local_conn_id,
                MAX_LOCAL_CONN_ID64
            );
            assert!(
                self.server_id <= MAX_SERVER_ID64,
                "unexpected serverID {} exceeds {}",
                self.server_id,
                MAX_SERVER_ID64
            );
            id |= 0x1;
            id |= self.local_conn_id << 1; // 40 bits local connID.
            id |= self.server_id << 41; // 22 bits serverID.
        } else {
            assert!(
                self.local_conn_id <= MAX_LOCAL_CONN_ID32,
                "unexpected localConnID {} exceeds {}",
                self.local_conn_id,
                MAX_LOCAL_CONN_ID32
            );
            assert!(
                self.server_id <= MAX_SERVER_ID32,
                "unexpected serverID {} exceeds {}",
                self.server_id,
                MAX_SERVER_ID32
            );
            id |= self.local_conn_id << 1; // 20 bits local connID.
            id |= self.server_id << 21; // 11 bits serverID.
        }
        id
    }
}

/// Error from [`parse_conn_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseConnIdError(&'static str);

impl fmt::Display for ParseConnIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ParseConnIdError {}

/// Parses a `u64` connection ID (Go `ParseConnID`). The boolean is
/// `isTruncated`: older clients truncated 64-bit IDs to 32 bits.
pub fn parse_conn_id(id: u64) -> Result<(Gcid, bool), ParseConnIdError> {
    if id & 0x8000_0000_0000_0000 > 0 {
        return Err(ParseConnIdError("unexpected connectionID exceeds int64"));
    }
    if id & 0x1 > 0 {
        // 64 bits
        if id & 0xffff_ffff_0000_0000 == 0 {
            return Ok((Gcid::default(), true));
        }
        return Ok((
            Gcid {
                is_64bits: true,
                local_conn_id: (id >> 1) & MAX_LOCAL_CONN_ID64,
                server_id: (id >> 41) & MAX_SERVER_ID64,
            },
            false,
        ));
    }

    // 32 bits
    if id & 0xffff_ffff_0000_0000 > 0 {
        return Err(ParseConnIdError("unexpected connectionID exceeds uint32"));
    }
    Ok((
        Gcid {
            is_64bits: false,
            local_conn_id: (id >> 1) & MAX_LOCAL_CONN_ID32,
            server_id: (id >> 21) & MAX_SERVER_ID32,
        },
        false,
    ))
}

/// Allocates global connection IDs (Go `Allocator`).
pub trait Allocator {
    /// Returns the next connection ID.
    fn next_id(&self) -> u64;
    /// Releases a connection ID back to the allocator.
    fn release(&self, connection_id: u64);
    /// Returns a reserved connection ID.
    fn get_reserved_conn_id(&self, reserved_no: u64) -> u64;
}

/// A simple allocator used when Global Kill is disabled (Go
/// `SimpleAllocator`).
pub struct SimpleAllocator {
    pool: AutoIncPool,
}

impl Default for SimpleAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleAllocator {
    /// Creates a new allocator.
    #[must_use]
    pub fn new() -> Self {
        let mut pool = AutoIncPool::default();
        pool.init(u64::MAX - RESERVED_COUNT);
        SimpleAllocator { pool }
    }
}

impl Allocator for SimpleAllocator {
    fn next_id(&self) -> u64 {
        self.pool.get().0
    }

    fn release(&self, id: u64) {
        self.pool.put(id);
    }

    fn get_reserved_conn_id(&self, reserved_no: u64) -> u64 {
        assert!(
            reserved_no < RESERVED_COUNT,
            "invalid reservedNo exceed ReservedCount"
        );
        u64::MAX - reserved_no
    }
}

/// The global connection ID allocator (Go `GlobalAllocator`).
pub struct GlobalAllocator {
    is_64bits: AtomicBool,
    server_id_getter: Box<dyn Fn() -> u64 + Send + Sync>,
    local32: LockFreeCircularPool,
    local64: AutoIncPool,
}

impl GlobalAllocator {
    /// Creates a global allocator (Go `NewGlobalAllocator`).
    pub fn new(
        server_id_getter: impl Fn() -> u64 + Send + Sync + 'static,
        enable_32bits: bool,
    ) -> Self {
        let mut local32 = LockFreeCircularPool::default();
        local32.init_ext(1 << LOCAL_CONN_ID_BITS32, u32::MAX);
        let mut local64 = AutoIncPool::default();
        local64.init_ext(
            (1 << LOCAL_CONN_ID_BITS64) - RESERVED_COUNT,
            true,
            LOCAL_CONN_ID_ALLOCATOR64_TRY_COUNT,
        );
        GlobalAllocator {
            is_64bits: AtomicBool::new(!enable_32bits),
            server_id_getter: Box::new(server_id_getter),
            local32,
            local64,
        }
    }

    fn is64(&self) -> bool {
        self.is_64bits.load(SeqCst)
    }

    fn upgrade_to_64(&self) {
        self.is_64bits.store(true, SeqCst);
        tracing::info!("GlobalAllocator upgrade to 64 bits");
    }

    fn downgrade_to_32(&self) {
        self.is_64bits.store(false, SeqCst);
        tracing::info!("GlobalAllocator downgrade to 32 bits");
    }

    /// Allocates a new global connection ID (Go `Allocate`).
    pub fn allocate(&self) -> Gcid {
        let server_id = (self.server_id_getter)();

        // 32 bits.
        if !self.is64() && server_id <= MAX_SERVER_ID32 {
            let (local_conn_id, ok) = self.local32.get();
            if ok {
                return Gcid {
                    server_id,
                    local_conn_id,
                    is_64bits: false,
                };
            }
            self.upgrade_to_64(); // go on to 64 bits.
        }

        // 64 bits.
        let (local_conn_id, ok) = self.local64.get();
        assert!(
            ok,
            "Failed to allocate 64bits local connID after try {LOCAL_CONN_ID_ALLOCATOR64_TRY_COUNT} times. Should never happen",
        );
        Gcid {
            server_id,
            local_conn_id,
            is_64bits: true,
        }
    }
}

impl Allocator for GlobalAllocator {
    fn next_id(&self) -> u64 {
        self.allocate().to_conn_id()
    }

    fn get_reserved_conn_id(&self, reserved_no: u64) -> u64 {
        assert!(
            reserved_no < RESERVED_COUNT,
            "invalid reservedNo exceed ReservedCount"
        );
        let server_id = (self.server_id_getter)();
        Gcid {
            server_id,
            local_conn_id: (1 << LOCAL_CONN_ID_BITS64) - 1 - reserved_no,
            is_64bits: true,
        }
        .to_conn_id()
    }

    fn release(&self, connection_id: u64) {
        let (gcid, is_truncated) = match parse_conn_id(connection_id) {
            Ok(parsed) if !parsed.1 => parsed,
            other => {
                tracing::error!(
                    connection_id,
                    is_truncated = matches!(&other, Ok((_, true))),
                    "failed to ParseGlobalConnID"
                );
                return;
            }
        };
        debug_assert!(!is_truncated);

        if gcid.is_64bits {
            self.local64.put(gcid.local_conn_id);
        } else if self.local32.put(gcid.local_conn_id) {
            if self.local32.len() < self.local32.cap() / 2 {
                self.downgrade_to_32();
            }
        } else {
            tracing::error!(
                connection_id,
                local_conn_id = gcid.local_conn_id,
                "failed to release 32bits connection ID"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicU32, Ordering::SeqCst};
    use std::sync::{Arc, Barrier, Mutex};

    // Go `TestToConnID`.
    #[test]
    fn to_conn_id() {
        let ok64 = Gcid {
            is_64bits: true,
            server_id: 1001,
            local_conn_id: 123,
        };
        assert_eq!(ok64.to_conn_id(), (1001_u64 << 41) | (123 << 1) | 1);

        let ok32 = Gcid {
            is_64bits: false,
            server_id: 1001,
            local_conn_id: 123,
        };
        assert_eq!(ok32.to_conn_id(), (1001_u64 << 21) | (123 << 1));

        for bad in [
            Gcid {
                is_64bits: true,
                server_id: 1 << 22,
                local_conn_id: 123,
            },
            Gcid {
                is_64bits: true,
                server_id: 1001,
                local_conn_id: 1 << 40,
            },
            Gcid {
                is_64bits: false,
                server_id: 1 << 11,
                local_conn_id: 123,
            },
            Gcid {
                is_64bits: false,
                server_id: 1001,
                local_conn_id: 1 << 20,
            },
        ] {
            assert!(
                std::panic::catch_unwind(move || bad.to_conn_id()).is_err(),
                "{bad:?}"
            );
        }
    }

    // Go `TestGlobalConnID`.
    #[test]
    fn parse_conn_id_cases() {
        // exceeds int64
        assert!(parse_conn_id(0x8000_0000_0000_0321).is_err());

        // 64 bits truncated
        let (_, truncated) = parse_conn_id(101).unwrap();
        assert!(truncated);

        // 64 bits
        let id1 = (1001_u64 << 41) | (123 << 1) | 1;
        let (gcid1, truncated) = parse_conn_id(id1).unwrap();
        assert!(!truncated);
        assert_eq!(gcid1.server_id, 1001);
        assert_eq!(gcid1.local_conn_id, 123);
        assert!(gcid1.is_64bits);

        // exceeds uint32
        assert!(parse_conn_id(0x1_0000_0320).is_err());

        // 32 bits
        let id2 = (2002_u64 << 21) | (321 << 1);
        let (gcid2, truncated) = parse_conn_id(id2).unwrap();
        assert!(!truncated);
        assert_eq!(gcid2.server_id, 2002);
        assert_eq!(gcid2.local_conn_id, 321);
        assert!(!gcid2.is_64bits);
        assert_eq!(gcid2.to_conn_id(), id2);
    }

    // Go `TestGetReservedConnID`.
    #[test]
    fn get_reserved_conn_id() {
        let simple = SimpleAllocator::new();
        assert_eq!(simple.get_reserved_conn_id(0), u64::MAX);
        assert_eq!(simple.get_reserved_conn_id(1), u64::MAX - 1);

        let global = GlobalAllocator::new(|| 1001, true);
        let max_local: u64 = (1 << 40) - 1;
        assert_eq!(
            global.get_reserved_conn_id(0),
            (1001_u64 << 41) | (max_local << 1) | 1
        );
        assert_eq!(
            global.get_reserved_conn_id(1),
            (1001_u64 << 41) | ((max_local - 1) << 1) | 1
        );
    }

    // Go `TestAutoIncPool`.
    #[test]
    fn auto_inc_pool() {
        const SIZE: u64 = 1 << 8;
        const TRY_CNT: usize = 4;

        let mut pool = AutoIncPool::default();
        pool.init_ext(SIZE, true, TRY_CNT);
        assert_eq!(pool.cap(), SIZE as i64);
        assert_eq!(pool.len(), 0);

        // get all.
        for i in 1..SIZE {
            let (val, ok) = pool.get();
            assert!(ok);
            assert_eq!(val, i);
        }
        let (val, ok) = pool.get();
        assert!(ok);
        assert_eq!(val, 0); // wrap around to 0
        assert_eq!(pool.len(), SIZE as i64);

        // exhausted: tries TRY_CNT times, advancing lastID to 0+TRY_CNT.
        assert!(!pool.get().1);

        let mut next_val = TRY_CNT as u64 + 1;
        pool.put(next_val);
        let (val, ok) = pool.get();
        assert!(ok);
        assert_eq!(val, next_val);

        next_val += TRY_CNT as u64 - 1;
        pool.put(next_val);
        let (val, ok) = pool.get();
        assert!(ok);
        assert_eq!(val, next_val);

        next_val += TRY_CNT as u64 + 1;
        pool.put(next_val);
        assert!(!pool.get().1);
    }

    // Go `TestLockFreePoolBasic`.
    #[test]
    fn lock_free_pool_basic() {
        const SIZE_IN_BITS: u32 = 8;
        const SIZE: u64 = (1 << SIZE_IN_BITS) - 1;

        let mut pool = LockFreeCircularPool::default();
        pool.init_ext(1 << SIZE_IN_BITS, u32::MAX);
        assert_eq!(pool.cap(), SIZE as i64);
        assert_eq!(pool.len(), SIZE as i64);

        for i in 1..=SIZE {
            let (val, ok) = pool.get();
            assert!(ok);
            assert_eq!(val, i);
        }
        assert!(!pool.get().1);
        assert_eq!(pool.len(), 0);

        for i in 1..=SIZE {
            assert!(pool.put(i));
        }
        assert!(!pool.put(0));
        assert_eq!(pool.len(), SIZE as i64);

        for i in 1..=SIZE {
            let (val, ok) = pool.get();
            assert!(ok);
            assert_eq!(val, i);
        }
        assert!(!pool.get().1);
        assert_eq!(pool.len(), 0);
    }

    // Go `TestLockFreePoolInitEmpty`.
    #[test]
    fn lock_free_pool_init_empty() {
        const SIZE_IN_BITS: u32 = 8;
        const SIZE: u64 = (1 << SIZE_IN_BITS) - 1;

        let mut pool = LockFreeCircularPool::default();
        pool.init_ext(1 << SIZE_IN_BITS, 0);
        assert_eq!(pool.cap(), SIZE as i64);
        assert_eq!(pool.len(), 0);

        for i in 1..=SIZE {
            assert!(pool.put(i));
        }
        assert!(!pool.put(0));
        assert_eq!(pool.len(), SIZE as i64);

        for i in 1..=SIZE {
            let (val, ok) = pool.get();
            assert!(ok);
            assert_eq!(val, i);
        }
        assert!(!pool.get().1);
        assert_eq!(pool.len(), 0);
    }

    /// The Go concurrency harness (`testLockFreePoolConcurrency`): producers
    /// push `0..requests` (spinning on full), consumers drain and sum until
    /// producers finish; the drained sum must equal the pushed sum plus the
    /// pre-fill. Go drives 20..1000 goroutines with 2^20 requests; OS threads
    /// are heavier, so thread counts are capped at 16 and requests at 2^16 —
    /// the same protocol contention, fewer schedulers.
    fn lock_free_pool_concurrency(
        size_in_bits: u32,
        fill_count: u32,
        producers: usize,
        consumers: usize,
        requests: u64,
        head_pos: u32,
    ) -> (i64, i64) {
        let mut pool = LockFreeCircularPool::default();
        pool.init_ext(1 << size_in_bits, fill_count);
        if head_pos > 0 {
            pool.init_for_test(head_pos, fill_count);
        }
        let pool = Arc::new(pool);
        let total = Arc::new(AtomicI64::new(0));
        let done = Arc::new(AtomicU32::new(0));
        let start = Arc::new(Barrier::new(producers + consumers));

        let mut handles = Vec::new();
        if producers > 0 {
            let reqs_per_producer = requests.div_ceil(producers as u64);
            for p in 0..producers as u64 {
                let pool = Arc::clone(&pool);
                let start = Arc::clone(&start);
                handles.push(std::thread::spawn(move || {
                    start.wait();
                    let lo = p * reqs_per_producer;
                    let hi = ((p + 1) * reqs_per_producer).min(requests);
                    for i in lo..hi {
                        while !pool.put(i) {
                            std::thread::yield_now();
                        }
                    }
                }));
            }
        }

        let mut consumer_handles = Vec::new();
        for _ in 0..consumers {
            let pool = Arc::clone(&pool);
            let total = Arc::clone(&total);
            let done = Arc::clone(&done);
            let start = Arc::clone(&start);
            consumer_handles.push(std::thread::spawn(move || {
                start.wait();
                let mut sum: i64 = 0;
                loop {
                    let (val, ok) = pool.get();
                    if ok {
                        sum += val as i64;
                        continue;
                    }
                    if done.load(SeqCst) == 1 {
                        break;
                    }
                    std::thread::yield_now();
                }
                total.fetch_add(sum, SeqCst);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        done.store(1, SeqCst);
        for h in consumer_handles {
            h.join().unwrap();
        }

        let mut expected: i64 = 0;
        if producers > 0 && consumers > 0 {
            expected += (requests as i64 - 1) * requests as i64 / 2;
        }
        if fill_count > 0 {
            let fill = fill_count.min((1 << size_in_bits) - 1) as i64;
            expected += (1 + fill) * fill / 2;
        }
        (expected, total.load(SeqCst))
    }

    // Go `TestLockFreePoolBasicConcurrencySafety` (incl. head/tail overflow).
    #[test]
    fn lock_free_pool_basic_concurrency_safety() {
        const SIZE_IN_BITS: u32 = 8;
        const REQUESTS: u64 = 1 << 16;
        let head_pos: u32 = 0u32.wrapping_sub(1 << (SIZE_IN_BITS + 8));

        let (expected, actual) = lock_free_pool_concurrency(SIZE_IN_BITS, 0, 16, 16, REQUESTS, 0);
        assert_eq!(expected, actual);

        // head & tail overflow across u32::MAX.
        let (expected, actual) =
            lock_free_pool_concurrency(SIZE_IN_BITS, 0, 16, 16, REQUESTS, head_pos);
        assert_eq!(expected, actual);
    }

    // Go `TestLockFreePoolConcurrencySafety`: the five queue-testing shapes
    // from Williams, "C++ Concurrency in Action, 2nd", 11.2.2.
    #[test]
    fn lock_free_pool_concurrency_safety_shapes() {
        const REQUESTS: u64 = 1 << 16;
        let cases: [(u32, u32, usize, usize); 5] = [
            (4, 1 << 3, 0, 16),        // pop-only, partially full
            (16, 0, 16, 1),            // many push, one pop, empty
            (16, 0xffff_ffff, 16, 1),  // many push, one pop, full
            (16, 0, 16, 16),           // many push, many pop, empty
            (16, 0xffff_ffff, 16, 16), // many push, many pop, full
        ];
        for (i, (size_in_bits, fill_count, producers, consumers)) in cases.into_iter().enumerate() {
            let (expected, actual) = lock_free_pool_concurrency(
                size_in_bits,
                fill_count,
                producers,
                consumers,
                REQUESTS,
                0,
            );
            assert_eq!(expected, actual, "case #{}", i + 1);
        }
    }

    // Go `TestLockBasedPoolConcurrencySafety` exercises a test-only lock-based
    // reference pool; the Rust reference is a Mutex<VecDeque> driven through
    // the same harness contract.
    #[test]
    fn lock_based_reference_pool_concurrency_safety() {
        const REQUESTS: u64 = 1 << 14;
        let pool = Arc::new(Mutex::new(std::collections::VecDeque::<u64>::new()));
        const CAPACITY: usize = (1 << 8) - 1;
        let total = Arc::new(AtomicI64::new(0));
        let done = Arc::new(AtomicU32::new(0));

        let mut producers = Vec::new();
        for p in 0..8u64 {
            let pool = Arc::clone(&pool);
            producers.push(std::thread::spawn(move || {
                let reqs = REQUESTS / 8;
                for i in (p * reqs)..((p + 1) * reqs) {
                    loop {
                        {
                            let mut q = pool.lock().unwrap();
                            if q.len() < CAPACITY {
                                q.push_back(i);
                                break;
                            }
                        }
                        std::thread::yield_now();
                    }
                }
            }));
        }
        let mut consumers = Vec::new();
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            let total = Arc::clone(&total);
            let done = Arc::clone(&done);
            consumers.push(std::thread::spawn(move || {
                let mut sum = 0i64;
                loop {
                    let popped = pool.lock().unwrap().pop_front();
                    match popped {
                        Some(v) => sum += v as i64,
                        None => {
                            if done.load(SeqCst) == 1 {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }
                total.fetch_add(sum, SeqCst);
            }));
        }
        for h in producers {
            h.join().unwrap();
        }
        done.store(1, SeqCst);
        for h in consumers {
            h.join().unwrap();
        }
        assert_eq!(
            total.load(SeqCst),
            (REQUESTS as i64 - 1) * REQUESTS as i64 / 2
        );
    }

    // Allocator behavior across the 32->64 upgrade and release/downgrade path,
    // covered in Go only implicitly via the server; pinned here.
    #[test]
    fn global_allocator_upgrade_and_release() {
        let global = GlobalAllocator::new(|| 7, true);

        let id = global.next_id();
        let (gcid, truncated) = parse_conn_id(id).unwrap();
        assert!(!truncated);
        assert!(!gcid.is_64bits);
        assert_eq!(gcid.server_id, 7);
        assert_eq!(gcid.local_conn_id, 1);

        global.release(id);
        let id2 = global.next_id();
        let (gcid2, _) = parse_conn_id(id2).unwrap();
        assert!(!gcid2.is_64bits);
        assert_eq!(gcid2.local_conn_id, 2);

        // A server ID beyond the 11-bit space forces the 64-bit layout.
        let wide = GlobalAllocator::new(|| MAX_SERVER_ID32 + 1, true);
        let id3 = wide.next_id();
        let (gcid3, _) = parse_conn_id(id3).unwrap();
        assert!(gcid3.is_64bits);
        assert_eq!(gcid3.server_id, MAX_SERVER_ID32 + 1);
    }
}
