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

//! Transcreation of Go `pkg/util/globalconn/pool.go`.
//!
//! Go's atomics are all sequentially consistent, so every atomic here uses
//! `SeqCst`; `runtime.Gosched()` maps to [`std::thread::yield_now`]. The
//! manual padding fields Go inserts against false sharing become
//! `#[repr(align(64))]` wrappers. Slot values are `AtomicU32` (relaxed data
//! guarded by the `seq` protocol) because Rust forbids the plain racing store
//! Go performs under the same protocol.

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::Mutex;

/// Invalid value from an [`IdPool`] (Go `IDPoolInvalidValue`).
pub const ID_POOL_INVALID_VALUE: u64 = u64::MAX;

/// The pool allocating & deallocating IDs (Go `IDPool`).
pub trait IdPool: fmt::Display {
    /// Initiates the pool.
    fn init(&mut self, size: u64);
    /// Returns the number of available IDs, or `-1` when unsupported.
    fn len(&self) -> i64;
    /// Returns the capacity of the pool.
    fn cap(&self) -> i64;
    /// Puts a value into the pool; `false` when the pool is full.
    fn put(&self, val: u64) -> bool;
    /// Gets a value from the pool; `false` when the pool is empty.
    fn get(&self) -> (u64, bool);
}

/// Auto-increment ID allocation; wrapping happens (Go `AutoIncPool`).
#[derive(Default)]
pub struct AutoIncPool {
    last_id: AtomicU64,
    cap: u64,
    try_cnt: usize,
    existed: Option<Mutex<HashSet<u64>>>,
}

impl AutoIncPool {
    /// Initiates the pool with more parameters (Go `InitExt`).
    pub fn init_ext(&mut self, size: u64, check_existed: bool, try_cnt: usize) {
        self.cap = size;
        if check_existed {
            self.existed = Some(Mutex::new(HashSet::new()));
        }
        self.try_cnt = try_cnt;
    }
}

impl IdPool for AutoIncPool {
    fn init(&mut self, size: u64) {
        self.init_ext(size, false, 1);
    }

    fn len(&self) -> i64 {
        match &self.existed {
            Some(existed) => existed.lock().unwrap().len() as i64,
            None => -1,
        }
    }

    fn cap(&self) -> i64 {
        self.cap as i64
    }

    fn put(&self, id: u64) -> bool {
        if let Some(existed) = &self.existed {
            existed.lock().unwrap().remove(&id);
        }
        true
    }

    fn get(&self) -> (u64, bool) {
        for _ in 0..self.try_cnt {
            let mut id = self.last_id.fetch_add(1, SeqCst).wrapping_add(1);
            if self.cap < u64::MAX {
                id %= self.cap;
            }
            if let Some(existed) = &self.existed {
                let mut existed = existed.lock().unwrap();
                if existed.contains(&id) {
                    continue;
                }
                existed.insert(id);
            }
            return (id, true);
        }
        (0, false)
    }
}

impl fmt::Display for AutoIncPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lastID: {}", self.last_id.load(SeqCst))
    }
}

/// A cache-line-aligned atomic, standing in for Go's manual padding fields.
#[repr(align(64))]
#[derive(Default)]
struct PaddedAtomicU32(AtomicU32);

struct LockFreePoolItem {
    value: AtomicU32,
    // seq indicates read/write status.
    // Sequence:
    //   seq==tail: writable -> doWrite,seq:=tail+1 -> seq==head+1: readable
    //   -> doRead,seq:=head+size -> (cycle)
    //   slot[i].seq: i(writable) -> i+1(readable) -> i+cap(writable) -> ...
    seq: AtomicU32,
}

/// A lock-free circular [`IdPool`]. To reduce memory usage it supports 32-bit
/// IDs only (Go `LockFreeCircularPool`).
#[derive(Default)]
pub struct LockFreeCircularPool {
    head: PaddedAtomicU32, // first available slot
    tail: PaddedAtomicU32, // first empty slot; head==tail means empty
    cap: u32,
    slots: Vec<LockFreePoolItem>,
}

impl LockFreeCircularPool {
    /// Initializes with `fill_count` pre-filled IDs `[1, min(fill_count,
    /// cap-1)]`; pass `u32::MAX` to fill the pool (Go `InitExt`).
    pub fn init_ext(&mut self, size: u32, fill_count: u32) {
        self.cap = size;
        self.slots = Vec::with_capacity(size as usize);

        let fill_count = fill_count.min(self.cap - 1);
        for i in 0..fill_count {
            self.slots.push(LockFreePoolItem {
                value: AtomicU32::new(i + 1),
                seq: AtomicU32::new(i + 1),
            });
        }
        for i in fill_count..self.cap {
            self.slots.push(LockFreePoolItem {
                value: AtomicU32::new(u32::MAX),
                seq: AtomicU32::new(i),
            });
        }

        self.head.0.store(0, SeqCst);
        self.tail.0.store(fill_count, SeqCst);
    }

    /// Re-bases the ring at `head` to unit-test head/tail overflow (Go
    /// `InitForTest`).
    pub fn init_for_test(&mut self, head: u32, fill_count: u32) {
        let fill_count = fill_count.min(self.cap - 1);
        for i in 0..fill_count {
            let slot = &self.slots[i as usize];
            slot.value.store(i + 1, SeqCst);
            slot.seq.store(head.wrapping_add(i + 1), SeqCst);
        }
        for i in fill_count..self.cap {
            let slot = &self.slots[i as usize];
            slot.value.store(u32::MAX, SeqCst);
            slot.seq.store(head.wrapping_add(i), SeqCst);
        }

        self.head.0.store(head, SeqCst);
        self.tail.0.store(head.wrapping_add(fill_count), SeqCst);
    }
}

impl IdPool for LockFreeCircularPool {
    fn init(&mut self, size: u64) {
        self.init_ext(size as u32, 0);
    }

    fn len(&self) -> i64 {
        i64::from(
            self.tail
                .0
                .load(SeqCst)
                .wrapping_sub(self.head.0.load(SeqCst)),
        )
    }

    fn cap(&self) -> i64 {
        i64::from(self.cap - 1)
    }

    fn put(&self, val: u64) -> bool {
        loop {
            // `tail` must load before `head` to avoid "false full".
            let tail = self.tail.0.load(SeqCst);
            let head = self.head.0.load(SeqCst);

            if tail.wrapping_sub(head) == self.cap - 1 {
                return false; // full
            }

            if self
                .tail
                .0
                .compare_exchange(tail, tail.wrapping_add(1), SeqCst, SeqCst)
                .is_err()
            {
                continue;
            }

            let slot = &self.slots[(tail & (self.cap - 1)) as usize];
            loop {
                let seq = slot.seq.load(SeqCst);
                if seq == tail {
                    // writable
                    slot.value.store(val as u32, SeqCst);
                    slot.seq.store(tail.wrapping_add(1), SeqCst);
                    return true;
                }
                std::thread::yield_now();
            }
        }
    }

    fn get(&self) -> (u64, bool) {
        loop {
            let head = self.head.0.load(SeqCst);
            let tail = self.tail.0.load(SeqCst);
            if head == tail {
                return (ID_POOL_INVALID_VALUE, false); // empty
            }

            if self
                .head
                .0
                .compare_exchange(head, head.wrapping_add(1), SeqCst, SeqCst)
                .is_err()
            {
                continue;
            }

            let slot = &self.slots[(head & (self.cap - 1)) as usize];
            loop {
                let seq = slot.seq.load(SeqCst);
                if seq == head.wrapping_add(1) {
                    // readable
                    let val = u64::from(slot.value.load(SeqCst));
                    slot.value.store(u32::MAX, SeqCst);
                    slot.seq.store(head.wrapping_add(self.cap), SeqCst);
                    return (val, true);
                }
                std::thread::yield_now();
            }
        }
    }
}

impl fmt::Display for LockFreeCircularPool {
    // Not thread safe, mirroring the source's notice.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let head = self.head.0.load(SeqCst);
        let tail = self.tail.0.load(SeqCst);
        let head_slot = &self.slots[(head & (self.cap - 1)) as usize];
        let tail_slot = &self.slots[(tail & (self.cap - 1)) as usize];
        let length = tail.wrapping_sub(head);
        write!(
            f,
            "cap:{}, length:{}; head:{:x}, slot:{{{:x},{:x}}}; tail:{:x}, slot:{{{:x},{:x}}}",
            self.cap,
            length,
            head,
            head_slot.value.load(SeqCst),
            head_slot.seq.load(SeqCst),
            tail,
            tail_slot.value.load(SeqCst),
            tail_slot.seq.load(SeqCst),
        )
    }
}
