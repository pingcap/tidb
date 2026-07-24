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

//! Complete transcreation of Go `pkg/util/bitmap` (`concurrent.go`).
//!
//! A static-length bitmap that is thread-safe on setting, implemented with CAS
//! because Go lacks atomic bitwise ops (golang/go#24244). Go stores the bits in
//! a `[]uint32` and drives it with `atomic.LoadUint32` /
//! `atomic.CompareAndSwapUint32` on `&segments[i]`. Rust's atomic operations are
//! methods on the atomic type, not on a pointer to a plain integer, so the
//! faithful mapping of that `[]uint32` is [`Vec<AtomicU32>`]: the atomic `Set`
//! path uses `compare_exchange`, and the deliberately non-atomic `UnsafeSet` /
//! `UnsafeIsSet` paths use exclusive `get_mut` / a relaxed load, which preserve
//! the "not thread-safe" contract of the originals.
//!
//! Bit ordering matches the source exactly: bit `i` maps to
//! `bitMask >> (i % 32)` within segment `i >> 5`, so bit 0 is the most
//! significant bit of segment 0. `BytesConsumed` mirrors
//! `unsafe.Sizeof(ConcurrentBitmap{}) + 4*cap(segments)` via
//! [`std::mem::size_of`]; both the Go struct (slice header + int) and this Rust
//! struct (`Vec` header + `usize`) are 32 bytes on a 64-bit target.
//!
//! `main_test.go` is a goroutine-leak `TestMain` (`goleak.VerifyTestMain`) with
//! no observable behavior of its own; it has no Rust equivalent.

use std::sync::atomic::{AtomicU32, Ordering};

const SEGMENT_WIDTH: usize = 32;
const SEGMENT_WIDTH_POWER: u32 = 5;
const BIT_MASK: u32 = 0x8000_0000;

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
        let segment_len = (bit_len + SEGMENT_WIDTH - 1) >> SEGMENT_WIDTH_POWER;
        let mut segments = Vec::with_capacity(segment_len);
        for _ in 0..segment_len {
            segments.push(AtomicU32::new(0));
        }
        Self { segments, bit_len }
    }

    /// Cleans the bitmap if the length is suitable, otherwise renewing one.
    pub fn reset(&mut self, bit_len: usize) {
        let segment_len = (bit_len + SEGMENT_WIDTH - 1) >> SEGMENT_WIDTH_POWER;
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
    /// A `bit_index` bigger than the initialized bit length returns false. This
    /// method is not thread-safe as it does not use an atomic load.
    #[must_use]
    pub fn unsafe_is_set(&self, bit_index: i64) -> bool {
        if bit_index < 0 || bit_index >= self.bit_len as i64 {
            return false;
        }

        let mask = BIT_MASK >> (bit_index % (SEGMENT_WIDTH as i64)) as u32;
        self.segments[(bit_index >> SEGMENT_WIDTH_POWER) as usize].load(Ordering::Relaxed) & mask
            != 0
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    // Go `TestConcurrentBitmapSet`.
    #[test]
    fn concurrent_bitmap_set() {
        const LOOP_COUNT: usize = 1000;
        const INTERVAL: usize = 2;

        let bm = Arc::new(ConcurrentBitmap::new(LOOP_COUNT * INTERVAL));
        let mut handles = Vec::with_capacity(LOOP_COUNT);
        for i in 0..LOOP_COUNT {
            let bm = Arc::clone(&bm);
            handles.push(thread::spawn(move || {
                bm.set((i * INTERVAL) as i64);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

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
    // bit. Reproducing 500k goroutines with OS threads is impractical, so a
    // fixed pool of `competitorsPerSet` worker threads is driven through a
    // per-iteration barrier: the same CAS contention on `set(31)`, without the
    // spawn storm. The main thread clears the bit between rounds; the two source
    // assertions (`clear_counter < loop_count` and
    // `setter_counter == clear_counter + 1`) hold.
    #[test]
    fn concurrent_bitmap_unique_setter() {
        const LOOP_COUNT: usize = 10000;
        const COMPETITORS_PER_SET: usize = 50;

        let bm = Arc::new(ConcurrentBitmap::new(32));
        let setter_counter = Arc::new(AtomicU64::new(0));
        let clear_counter = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(COMPETITORS_PER_SET + 1));
        let done = Arc::new(Barrier::new(COMPETITORS_PER_SET + 1));

        let mut handles = Vec::with_capacity(COMPETITORS_PER_SET);
        for _ in 0..COMPETITORS_PER_SET {
            let bm = Arc::clone(&bm);
            let setter_counter = Arc::clone(&setter_counter);
            let start = Arc::clone(&start);
            let done = Arc::clone(&done);
            handles.push(thread::spawn(move || {
                for _ in 0..LOOP_COUNT {
                    start.wait();
                    if bm.set(31) {
                        setter_counter.fetch_add(1, Ordering::SeqCst);
                    }
                    done.wait();
                }
            }));
        }

        for _ in 0..LOOP_COUNT {
            // Clear bitmap to zero.
            if bm.segments[0]
                .compare_exchange(0x0000_0001, 0x0000_0000, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                clear_counter.fetch_add(1, Ordering::SeqCst);
            }
            // Release the workers to set, then wait for the round to finish.
            start.wait();
            done.wait();
        }
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
}
