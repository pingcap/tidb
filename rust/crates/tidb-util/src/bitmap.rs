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

//! Complete transcreation of Go `pkg/util/bitmap`.
//!
//! The bitmap has a fixed logical length, ignores out-of-range indexes, and
//! lets concurrent callers race to set a bit while reporting exactly one
//! winner. [`AtomicU32`] provides the Rust-native segment authority; reset,
//! clone, direct single-owner access, and memory accounting preserve the
//! package's observable contracts.

use std::sync::atomic::{AtomicU32, Ordering};

const SEGMENT_WIDTH: usize = 32;
const SEGMENT_WIDTH_POWER: u32 = 5;
const BIT_MASK: u32 = 0x8000_0000;

fn segment_len(bit_len: usize) -> usize {
    let rounded_bit_len = bit_len
        .checked_add(SEGMENT_WIDTH - 1)
        .filter(|rounded| *rounded <= isize::MAX as usize)
        .expect("bitmap bit length exceeds the source int domain");
    rounded_bit_len >> SEGMENT_WIDTH_POWER
}

/// A static-length bitmap which is thread-safe on setting.
///
/// It is implemented using CAS, as atomic bitwise operation is not supported by
/// Go yet (see <https://github.com/golang/go/issues/24244>). CAS operation is
/// narrowed down to `u32` instead of longer types like `u64`, to reduce
/// probability of racing.
pub struct ConcurrentBitmap {
    segments: Vec<AtomicU32>,
    bit_len: usize,
}

impl ConcurrentBitmap {
    /// Initializes a `ConcurrentBitmap` which can store `bit_len` of bits.
    #[must_use]
    pub fn new(bit_len: usize) -> Self {
        let segment_len = segment_len(bit_len);
        let mut segments = Vec::with_capacity(segment_len);
        for _ in 0..segment_len {
            segments.push(AtomicU32::new(0));
        }
        Self { segments, bit_len }
    }

    /// Cleans the bitmap if the length is suitable, otherwise renewing one.
    pub fn reset(&mut self, bit_len: usize) {
        let segment_len = segment_len(bit_len);
        if segment_len <= self.segments.len() {
            for seg in &self.segments {
                seg.store(0, Ordering::Relaxed);
            }
            self.bit_len = bit_len;
        } else {
            let mut segments = Vec::with_capacity(segment_len);
            for _ in 0..segment_len {
                segments.push(AtomicU32::new(0));
            }
            self.segments = segments;
            self.bit_len = bit_len;
        }
    }

    /// Returns size of this bitmap in bytes.
    #[must_use]
    pub fn bytes_consumed(&self) -> i64 {
        std::mem::size_of::<ConcurrentBitmap>() as i64
            + (SEGMENT_WIDTH / 8 * self.segments.capacity()) as i64
    }

    /// Sets the bit on `bit_index` to be 1 (`bit_index` starts from 0).
    ///
    /// The returned value indicates whether this call triggers the bit from 0 to
    /// 1. A `bit_index` bigger than the initialized bit length is ignored.
    pub fn set(&self, bit_index: i64) -> bool {
        if bit_index < 0 || bit_index >= self.bit_len as i64 {
            return false;
        }

        let segment = &self.segments[(bit_index >> SEGMENT_WIDTH_POWER) as usize];
        let mask = BIT_MASK >> (bit_index % (SEGMENT_WIDTH as i64)) as u32;
        // Repeatedly observe whether the bit is already set, and try to set it
        // based on that observation.
        loop {
            let old_value = segment.load(Ordering::SeqCst);
            if old_value & mask != 0 {
                return false;
            }

            let new_value = old_value | mask;
            if segment
                .compare_exchange(old_value, new_value, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Sets the bit on `bit_index` to be 1 (`bit_index` starts from 0).
    ///
    /// A `bit_index` bigger than the initialized bit length is ignored. This
    /// version is concurrent-unsafe; the caller must ensure the write happens on
    /// a single thread, which `&mut self` enforces.
    pub fn unsafe_set(&mut self, bit_index: i64) {
        if bit_index < 0 || bit_index >= self.bit_len as i64 {
            return;
        }

        let mask = BIT_MASK >> (bit_index % (SEGMENT_WIDTH as i64)) as u32;
        let seg = self.segments[(bit_index >> SEGMENT_WIDTH_POWER) as usize].get_mut();
        *seg |= mask;
    }

    /// Returns whether the bit on `bit_index` is set (`bit_index` starts from 0).
    ///
    /// A `bit_index` bigger than the initialized bit length returns false. The
    /// exclusive receiver enforces the source method's non-concurrent access
    /// contract.
    #[must_use]
    pub fn unsafe_is_set(&mut self, bit_index: i64) -> bool {
        if bit_index < 0 || bit_index >= self.bit_len as i64 {
            return false;
        }

        let mask = BIT_MASK >> (bit_index % (SEGMENT_WIDTH as i64)) as u32;
        *self.segments[(bit_index >> SEGMENT_WIDTH_POWER) as usize].get_mut() & mask != 0
    }
}

/// Clones a new bitmap with the old bits set.
impl Clone for ConcurrentBitmap {
    fn clone(&self) -> Self {
        let cp = Self::new(self.bit_len);
        for (dst, src) in cp.segments.iter().zip(self.segments.iter()) {
            dst.store(src.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        cp
    }
}

#[cfg(test)]
mod tests {
    use super::ConcurrentBitmap;
    use crossbeam_channel::bounded;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;

    // Go `TestConcurrentBitmapSet`.
    #[test]
    fn concurrent_bitmap_set() {
        const LOOP_COUNT: usize = 1000;
        const INTERVAL: usize = 2;
        const WORKERS: usize = 16;

        let bm = Arc::new(ConcurrentBitmap::new(LOOP_COUNT * INTERVAL));
        let mut handles = Vec::with_capacity(WORKERS);
        for worker in 0..WORKERS {
            let bm = Arc::clone(&bm);
            handles.push(thread::spawn(move || {
                for i in (worker..LOOP_COUNT).step_by(WORKERS) {
                    bm.set((i * INTERVAL) as i64);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut bm = Arc::try_unwrap(bm)
            .ok()
            .expect("all bitmap worker references should be dropped");
        for i in 0..LOOP_COUNT {
            if i % INTERVAL == 0 {
                assert!(bm.unsafe_is_set(i as i64));
            } else {
                assert!(!bm.unsafe_is_set(i as i64));
            }
        }
    }

    // Go `TestConcurrentBitmapUniqueSetter` checks that `isSetter` is unique
    // every time a bit is set.
    //
    // Go spawns `competitorsPerSet` goroutines per iteration (500k total), all
    // racing `Set(31)`, while the main goroutine interleaves CAS-clears of the
    // bit. Reproducing 500k goroutines with OS threads is impractical, so 50
    // persistent workers consume the same 500k progressively submitted calls.
    // The bounded queue lets the producer advance and clear while older setters
    // remain unfinished, preserving the source race and both counter invariants.
    #[test]
    fn concurrent_bitmap_unique_setter() {
        const LOOP_COUNT: usize = 10000;
        const COMPETITORS_PER_SET: usize = 50;

        let bm = Arc::new(ConcurrentBitmap::new(32));
        let setter_counter = Arc::new(AtomicU64::new(0));
        let clear_counter = AtomicU64::new(0);
        let (work_tx, work_rx) = bounded::<()>(COMPETITORS_PER_SET);

        let mut handles = Vec::with_capacity(COMPETITORS_PER_SET);
        for _ in 0..COMPETITORS_PER_SET {
            let bm = Arc::clone(&bm);
            let setter_counter = Arc::clone(&setter_counter);
            let work_rx = work_rx.clone();
            handles.push(thread::spawn(move || {
                for () in work_rx {
                    if bm.set(31) {
                        setter_counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
        }
        drop(work_rx);

        for _ in 0..LOOP_COUNT {
            // Clear bitmap to zero.
            if bm.segments[0]
                .compare_exchange(0x0000_0001, 0x0000_0000, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                clear_counter.fetch_add(1, Ordering::SeqCst);
            }
            for _ in 0..COMPETITORS_PER_SET {
                work_tx.send(()).unwrap();
            }
        }
        drop(work_tx);
        for h in handles {
            h.join().unwrap();
        }

        assert!(clear_counter.load(Ordering::SeqCst) < LOOP_COUNT as u64);
        assert_eq!(
            setter_counter.load(Ordering::SeqCst),
            clear_counter.load(Ordering::SeqCst) + 1
        );
    }

    // Go `TestResetConcurrentBitmap`.
    #[test]
    fn reset_concurrent_bitmap() {
        let mut bm = ConcurrentBitmap::new(32);
        bm.set(1);
        bm.set(3);
        bm.set(7);
        bm.set(16);
        bm.reset(8);
        assert_eq!(bm.bit_len, 8);
        assert!(!bm.unsafe_is_set(1));
        assert!(!bm.unsafe_is_set(3));
        assert!(!bm.unsafe_is_set(7));
    }

    #[test]
    fn public_bounds_bit_order_and_single_owner_access() {
        let mut bm = ConcurrentBitmap::new(33);

        assert!(!bm.set(-1));
        assert!(!bm.set(33));
        bm.unsafe_set(-1);
        bm.unsafe_set(33);
        assert!(!bm.unsafe_is_set(-1));
        assert!(!bm.unsafe_is_set(33));

        assert!(bm.set(0));
        assert!(!bm.set(0));
        bm.unsafe_set(31);
        bm.unsafe_set(32);
        assert_eq!(bm.segments[0].load(Ordering::Relaxed), 0x8000_0001);
        assert_eq!(bm.segments[1].load(Ordering::Relaxed), 0x8000_0000);
        assert_eq!(
            bm.bytes_consumed(),
            (std::mem::size_of::<ConcurrentBitmap>()
                + bm.segments.capacity() * std::mem::size_of::<u32>()) as i64
        );
    }

    #[test]
    fn clone_is_independent_and_reset_reuses_or_grows_storage() {
        let mut bm = ConcurrentBitmap::new(64);
        bm.unsafe_set(0);
        bm.unsafe_set(63);
        let mut clone = bm.clone();

        bm.unsafe_set(31);
        assert!(clone.unsafe_is_set(0));
        assert!(clone.unsafe_is_set(63));
        assert!(!clone.unsafe_is_set(31));

        bm.reset(1);
        assert_eq!(bm.segments.len(), 2);
        assert!(!bm.unsafe_is_set(0));

        bm.reset(65);
        assert_eq!(bm.segments.len(), 3);
        assert!(!bm.unsafe_is_set(64));
    }

    #[test]
    fn oversized_length_is_rejected_without_release_wraparound() {
        assert!(std::panic::catch_unwind(|| ConcurrentBitmap::new(usize::MAX)).is_err());
        assert!(std::panic::catch_unwind(|| {
            let mut bm = ConcurrentBitmap::new(1);
            bm.reset(usize::MAX);
        })
        .is_err());
    }
}
