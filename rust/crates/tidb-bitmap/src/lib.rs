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

use zerocopy::FromZeros;

const SEGMENT_WIDTH: usize = 32;
const SEGMENT_WIDTH_POWER: u32 = 5;
const BIT_MASK: u32 = 0x8000_0000;
const INLINE_SEGMENTS: usize = 8;

fn segment_len(bit_len: isize) -> isize {
    bit_len.wrapping_add(SEGMENT_WIDTH as isize - 1) >> SEGMENT_WIDTH_POWER
}

/// A fixed-length bitmap that uses 32-bit compare-and-swap for concurrent sets.
pub struct ConcurrentBitmap {
    // The first eight segments stay inline. The vector holds any remaining
    // segments. Reset retains the allocated counts even when the bitmap shrinks.
    segments: Vec<AtomicU32>,
    bit_len: isize,
    inline: [AtomicU32; INLINE_SEGMENTS],
    inline_len: usize,
}

impl ConcurrentBitmap {
    /// Initializes a `ConcurrentBitmap` which can store `bit_len` of bits.
    pub fn new(bit_len: isize) -> Self {
        let count =
            usize::try_from(segment_len(bit_len)).expect("bitmap segment length is negative");
        Self {
            segments: AtomicU32::new_vec_zeroed(count.saturating_sub(INLINE_SEGMENTS))
                .expect("bitmap allocation failed"),
            bit_len,
            inline: [const { AtomicU32::new(0) }; INLINE_SEGMENTS],
            inline_len: count.min(INLINE_SEGMENTS),
        }
    }

    /// Cleans the bitmap if the length is suitable, otherwise renewing one.
    pub fn reset(&mut self, bit_len: isize) {
        let retained = self.segments.len() + self.inline_len;
        if segment_len(bit_len) <= retained as isize {
            for seg in &mut self.segments {
                *seg.get_mut() = 0;
            }
            self.inline = [const { AtomicU32::new(0) }; INLINE_SEGMENTS];
            self.bit_len = bit_len;
        } else {
            *self = Self::new(bit_len);
        }
    }

    /// Returns Go's memory accounting for the bitmap, including retained segments.
    pub fn bytes_consumed(&self) -> i64 {
        // Go's bitmap header consists of its three-word slice and the bit count.
        (4 * size_of::<usize>() + (SEGMENT_WIDTH / 8) * (self.segments.len() + self.inline_len))
            as i64
    }

    /// Sets the bit on `bit_index` to be 1 (`bit_index` starts from 0).
    ///
    /// The returned value indicates whether this call triggers the bit from 0 to
    /// 1. A `bit_index` bigger than the initialized bit length is ignored.
    pub fn set(&self, bit_index: isize) -> bool {
        if bit_index < 0 || bit_index >= self.bit_len {
            return false;
        }

        let index = (bit_index >> SEGMENT_WIDTH_POWER) as usize;
        let segment = if index < INLINE_SEGMENTS {
            &self.inline[index]
        } else {
            &self.segments[index - INLINE_SEGMENTS]
        };
        let mask = BIT_MASK >> (bit_index % SEGMENT_WIDTH as isize) as u32;
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
    #[inline]
    pub fn unsafe_set(&mut self, bit_index: isize) {
        if bit_index < 0 || bit_index >= self.bit_len {
            return;
        }

        let mask = BIT_MASK >> (bit_index % SEGMENT_WIDTH as isize) as u32;
        *self.segment_mut(bit_index) |= mask;
    }

    /// Returns whether the bit on `bit_index` is set (`bit_index` starts from 0).
    ///
    /// A `bit_index` bigger than the initialized bit length returns false. The
    /// exclusive receiver enforces the source method's non-concurrent access
    /// contract.
    #[inline]
    pub fn unsafe_is_set(&mut self, bit_index: isize) -> bool {
        if bit_index < 0 || bit_index >= self.bit_len {
            return false;
        }

        let mask = BIT_MASK >> (bit_index % SEGMENT_WIDTH as isize) as u32;
        *self.segment_mut(bit_index) & mask != 0
    }

    fn segment_mut(&mut self, bit_index: isize) -> &mut u32 {
        let index = (bit_index >> SEGMENT_WIDTH_POWER) as usize;
        if index < INLINE_SEGMENTS {
            self.inline[index].get_mut()
        } else {
            self.segments[index - INLINE_SEGMENTS].get_mut()
        }
    }
}

/// Clones a new bitmap with the old bits set.
impl Clone for ConcurrentBitmap {
    fn clone(&self) -> Self {
        let count =
            usize::try_from(segment_len(self.bit_len)).expect("bitmap segment length is negative");
        let segments = self.segments[..count.saturating_sub(INLINE_SEGMENTS)]
            .iter()
            .map(|segment| AtomicU32::new(segment.load(Ordering::Relaxed)))
            .collect();
        let inline_len = count.min(INLINE_SEGMENTS);
        let mut inline = [const { AtomicU32::new(0) }; INLINE_SEGMENTS];
        for (dst, src) in inline.iter_mut().zip(&self.inline[..inline_len]) {
            *dst.get_mut() = src.load(Ordering::Relaxed);
        }
        Self {
            segments,
            bit_len: self.bit_len,
            inline,
            inline_len,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::ConcurrentBitmap;
    use crossbeam_channel::bounded;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    #[test]
    fn TestConcurrentBitmapSet() {
        const LOOP_COUNT: usize = 1000;
        const INTERVAL: usize = 2;
        const WORKERS: usize = 16;

        let bm = Arc::new(ConcurrentBitmap::new((LOOP_COUNT * INTERVAL) as isize));
        let mut handles = Vec::with_capacity(WORKERS);
        for worker in 0..WORKERS {
            let bm = Arc::clone(&bm);
            handles.push(thread::spawn(move || {
                for i in (worker..LOOP_COUNT).step_by(WORKERS) {
                    bm.set((i * INTERVAL) as isize);
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
                assert!(bm.unsafe_is_set(i as isize));
            } else {
                assert!(!bm.unsafe_is_set(i as isize));
            }
        }
    }

    // Go spawns `competitorsPerSet` goroutines per iteration (500k total), all
    // racing `Set(31)`, while the main goroutine interleaves CAS-clears of the
    // bit. Reproducing 500k goroutines with OS threads is impractical, so 50
    // persistent workers consume the same 500k progressively submitted calls.
    // The bounded queue lets the producer advance and clear while older setters
    // remain unfinished, preserving the source race and both counter invariants.
    #[test]
    fn TestConcurrentBitmapUniqueSetter() {
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
            if bm.inline[0]
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

    #[test]
    fn TestResetConcurrentBitmap() {
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
    fn clone_copies_logical_segments_across_the_inline_boundary() {
        let mut bitmap = ConcurrentBitmap::new(513);
        let retained = bitmap.bytes_consumed();
        assert!(bitmap.set(255));
        assert!(bitmap.set(256));
        let mut clone = bitmap.clone();
        assert!(clone.unsafe_is_set(255));
        assert!(clone.unsafe_is_set(256));
        assert!(!clone.unsafe_is_set(257));

        bitmap.reset(256);
        assert_eq!(bitmap.bytes_consumed(), retained);
        assert_eq!(
            bitmap.clone().bytes_consumed(),
            ConcurrentBitmap::new(256).bytes_consumed()
        );
        assert!(clone.unsafe_is_set(255));
        assert!(clone.unsafe_is_set(256));

        bitmap.reset(513);
        assert_eq!(bitmap.bytes_consumed(), retained);
        assert!(!bitmap.unsafe_is_set(255));
        assert!(!bitmap.unsafe_is_set(256));
    }
}
