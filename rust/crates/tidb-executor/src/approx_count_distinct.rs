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

//! A faithful port of Go `func_count_distinct.go`'s
//! `partialResult4ApproxCountDistinct`, the `BJKST` sketch behind
//! `APPROX_COUNT_DISTINCT`.
//!
//! Below `UNIQUES_HASH_MAX_SIZE` (65536) distinct 32-bit hash values the
//! sketch keeps every one of them and the count is exact. Above that it
//! starts discarding hashes not divisible by `2^skip_degree` and
//! extrapolates the true cardinality from the surviving fraction
//! (`fixed_size`); that extrapolation, and the exact bucket-eviction order
//! that produces it, is what this module ports bit for bit so results match
//! Go's for large-cardinality groups.

use crate::farmhash;

/// The maximum degree of buffer size before the values are discarded.
const UNIQUES_HASH_MAX_SIZE_DEGREE: u8 = 17;
/// The maximum number of elements before the values are discarded.
const UNIQUES_HASH_MAX_SIZE: u32 = 1u32 << (UNIQUES_HASH_MAX_SIZE_DEGREE - 1);
/// Initial buffer size degree.
const UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE: u8 = 4;
/// The number of least significant bits used for thinning. The remaining
/// high-order bits are used to determine the position in the hash table.
const UNIQUES_HASH_BITS_FOR_SKIP: u32 = 32 - UNIQUES_HASH_MAX_SIZE_DEGREE as u32;

/// Go `intHash64`: a Murmur-style 64-to-64 mix used to derive a
/// pseudo-random remainder in `fixed_size`.
fn int_hash64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

/// Go `partialResult4ApproxCountDistinct`, the BJKST sketch's mutable state.
#[derive(Clone, Debug)]
pub struct ApproxCountDistinctSketch {
    size: u32,
    size_degree: u8,
    skip_degree: u8,
    has_zero: bool,
    buf: Vec<u32>,
}

impl Default for ApproxCountDistinctSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxCountDistinctSketch {
    /// Go `NewPartialResult4ApproxCountDistinct`.
    pub fn new() -> Self {
        let mut sketch = ApproxCountDistinctSketch {
            size: 0,
            size_degree: 0,
            skip_degree: 0,
            has_zero: false,
            buf: Vec::new(),
        };
        sketch.reset();
        sketch
    }

    fn alloc(&mut self, new_size_degree: u8) {
        self.size = 0;
        self.skip_degree = 0;
        self.has_zero = false;
        self.buf = vec![0u32; 1usize << new_size_degree];
        self.size_degree = new_size_degree;
    }

    fn reset(&mut self) {
        self.alloc(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
    }

    fn buf_size(&self) -> u32 {
        1u32 << self.size_degree
    }

    fn mask(&self) -> u32 {
        self.buf_size() - 1
    }

    fn place(&self, x: u32) -> u32 {
        (x >> UNIQUES_HASH_BITS_FOR_SKIP) & self.mask()
    }

    /// Increase the size of the buffer 2 times or up to `new_size_degree`.
    fn resize(&mut self, mut new_size_degree: u8) {
        let old_size = self.buf_size();
        let old_buf = std::mem::take(&mut self.buf);

        if new_size_degree == 0 {
            new_size_degree = self.size_degree + 1;
        }

        self.buf = vec![0u32; 1usize << new_size_degree];
        self.size_degree = new_size_degree;

        // Move some items to new locations.
        for i in 0..old_size {
            let x = old_buf[i as usize];
            if x != 0 {
                self.reinsert_impl(x);
            }
        }
    }

    /// Go `hashValue >> skipDegree << skipDegree == hashValue`: the value is
    /// divided by `2 ^ skip_degree`.
    fn good(&self, hash: u32) -> bool {
        hash == ((hash >> self.skip_degree) << self.skip_degree)
    }

    /// Insert a value.
    fn insert_impl(&mut self, x: u32) {
        if x == 0 {
            if !self.has_zero {
                self.size += 1;
            }
            self.has_zero = true;
            return;
        }

        let mask = self.mask();
        let mut place_value = self.place(x);
        while self.buf[place_value as usize] != 0 && self.buf[place_value as usize] != x {
            place_value = (place_value + 1) & mask;
        }

        if self.buf[place_value as usize] == x {
            return;
        }

        self.buf[place_value as usize] = x;
        self.size += 1;
    }

    fn max_fill(&self) -> u32 {
        1u32 << (self.size_degree - 1)
    }

    /// If the hash table is full enough, then do resize. If there are too
    /// many items, then throw half the pieces until they are small enough.
    fn shrink_if_need(&mut self) {
        if self.size > self.max_fill() {
            if self.size > UNIQUES_HASH_MAX_SIZE {
                while self.size > UNIQUES_HASH_MAX_SIZE {
                    self.skip_degree += 1;
                    self.rehash();
                }
            } else {
                self.resize(0);
            }
        }
    }

    /// Delete all values whose hashes do not divide by `2 ^ skip_degree`.
    fn rehash(&mut self) {
        for i in 0..self.buf_size() {
            let idx = i as usize;
            if self.buf[idx] != 0 && !self.good(self.buf[idx]) {
                self.buf[idx] = 0;
                self.size -= 1;
            }
        }

        for i in 0..self.buf_size() {
            let idx = i as usize;
            if self.buf[idx] != 0 && i != self.place(self.buf[idx]) {
                let x = self.buf[idx];
                self.buf[idx] = 0;
                self.reinsert_impl(x);
            }
        }
    }

    /// Insert a value into the new buffer that was in the old buffer. Used
    /// when increasing the size of the buffer, as well as when reading from
    /// a file.
    fn reinsert_impl(&mut self, x: u32) {
        let mask = self.mask();
        let mut place_value = self.place(x);
        while self.buf[place_value as usize] != 0 {
            place_value = (place_value + 1) & mask;
        }
        self.buf[place_value as usize] = x;
    }

    fn insert_hash(&mut self, hash_value: u32) {
        if !self.good(hash_value) {
            return;
        }
        self.insert_impl(hash_value);
        self.shrink_if_need();
    }

    /// Go `InsertHash64`: no need to rehash, just cast into uint32.
    fn insert_hash64(&mut self, x: u64) {
        self.insert_hash(x as u32);
    }

    /// Hashes `encoded` with the same FarmHash `Hash64` Go uses
    /// (`farm.Hash64(encodedBytes)`) and folds it into the sketch.
    pub fn insert(&mut self, encoded: &[u8]) {
        let hash = farmhash::hash64(encoded);
        self.insert_hash64(hash);
    }

    /// Go `merge`: folds another sketch's surviving elements into `self`.
    ///
    /// Not yet called from `hash_agg.rs` -- this seed's `HashAggExec` is the
    /// serial `unparallelExec` path only (Go's parallel partial/final worker
    /// pipeline is a documented deferral) -- but it is part of the sketch's
    /// ported behavior and is exercised directly by this module's tests.
    #[allow(dead_code)]
    pub fn merge(&mut self, other: &ApproxCountDistinctSketch) {
        if other.skip_degree > self.skip_degree {
            self.skip_degree = other.skip_degree;
            self.rehash();
        }

        if !self.has_zero && other.has_zero {
            self.has_zero = true;
            self.size += 1;
            self.shrink_if_need();
        }

        for i in 0..other.buf_size() {
            let x = other.buf[i as usize];
            if x != 0 && self.good(x) {
                self.insert_impl(x);
                self.shrink_if_need();
            }
        }
    }

    /// Go `fixedSize`: corrects the systematic bias from hashing into a
    /// 32-bit space and, once elements have been thinned out
    /// (`skip_degree > 0`), extrapolates the true cardinality from the
    /// surviving sample.
    pub fn fixed_size(&self) -> u64 {
        if self.skip_degree == 0 {
            return self.size as u64;
        }

        let mut res = (self.size as u64) * (1u64 << self.skip_degree);

        // Pseudo-random remainder.
        res += int_hash64(self.size as u64) & ((1u64 << self.skip_degree) - 1);

        // When different elements randomly scattered across 2^32 buckets,
        // filled buckets with average of `res` obtained.
        let p32 = 1u64 << 32;
        let fixed_res = (p32 as f64) * ((p32 as f64).ln() - ((p32 - res) as f64).ln());
        fixed_res.round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_below_threshold() {
        let mut sketch = ApproxCountDistinctSketch::new();
        for i in 0..1000i64 {
            sketch.insert(&i.to_le_bytes());
        }
        assert_eq!(sketch.fixed_size(), 1000);
    }

    #[test]
    fn duplicate_insert_does_not_double_count() {
        let mut sketch = ApproxCountDistinctSketch::new();
        for _ in 0..5 {
            sketch.insert(&7i64.to_le_bytes());
        }
        assert_eq!(sketch.fixed_size(), 1);
    }

    /// Captured from Go (`testkit.CreateMockStore`,
    /// `pkg/executor/zz_dump_approxcount_test.go`, `-tags=intest`): a
    /// `BIGINT` column loaded with 0..100000 (then 0..70000) distinct
    /// values, `SELECT APPROX_COUNT_DISTINCT(v) FROM t`. Both are past the
    /// 65536-distinct-value threshold where the sketch starts discarding
    /// samples and extrapolating, so an exact match here is the proof the
    /// FarmHash port and the sketch's skip/resize/rehash arithmetic are
    /// bit-for-bit faithful -- Go got 101048 and 70697.
    #[test]
    fn large_cardinality_matches_go_capture() {
        let mut hundred_k = ApproxCountDistinctSketch::new();
        for i in 0..100_000i64 {
            hundred_k.insert(&i.to_le_bytes());
        }
        assert_eq!(hundred_k.fixed_size(), 101048);

        let mut seventy_k = ApproxCountDistinctSketch::new();
        for i in 0..70_000i64 {
            seventy_k.insert(&i.to_le_bytes());
        }
        assert_eq!(seventy_k.fixed_size(), 70697);
    }

    #[test]
    fn merge_of_disjoint_sketches_below_threshold() {
        let mut left = ApproxCountDistinctSketch::new();
        for i in 0..500i64 {
            left.insert(&i.to_le_bytes());
        }
        let mut right = ApproxCountDistinctSketch::new();
        for i in 500..900i64 {
            right.insert(&i.to_le_bytes());
        }
        left.merge(&right);
        assert_eq!(left.fixed_size(), 900);
    }
}
