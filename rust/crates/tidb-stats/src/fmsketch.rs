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

//! Raw hash-set Flajolet-Martin sketch geometry from
//! `pkg/statistics/fmsketch.go`.
//!
//! The Go owner also hashes `types.Datum` values through tablecodec, feeds
//! sketches from the row sampler, and persists them through tipb protobufs.
//! This leaf starts at an already-owned `u64` hash and therefore keeps those
//! datatype, sampler, protobuf, and statistics-handle seams explicit.

use std::collections::HashSet;

/// Go's fixed fallback used by `DecodeFMSketch` after protobuf restoration.
pub const MAX_SKETCH_SIZE: usize = 10_000;

/// The dependency-closed core of TiDB's FM sketch.
///
/// The sketch keeps unique hashes that satisfy the current mask.  When the
/// set grows past `max_size`, the mask advances from `m` to `2*m + 1` and
/// hashes that do not have all newly-required zero bits are removed.  This is
/// the exact level transition in Go, including wrapping `u64` arithmetic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FmSketch {
    hashes: HashSet<u64>,
    mask: u64,
    max_size: usize,
}

impl FmSketch {
    /// Creates an empty sketch with the source hash-set threshold.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            hashes: HashSet::with_capacity(max_size),
            mask: 0,
            max_size,
        }
    }

    /// Returns the current mask.  A value of `2^r - 1` means that hashes need
    /// at least `r` trailing zero bits to remain in the set.
    #[must_use]
    pub const fn mask(&self) -> u64 {
        self.mask
    }

    /// Returns the source hash-set threshold.
    #[must_use]
    pub const fn max_size(&self) -> usize {
        self.max_size
    }

    /// Returns the number of retained unique hashes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Returns whether no hash is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Returns a stable view of retained hashes for persistence owners.
    ///
    /// The source map is intentionally unordered; callers that serialize the
    /// set must choose and document their own deterministic ordering.
    #[must_use]
    pub fn contains(&self, hash: u64) -> bool {
        self.hashes.contains(&hash)
    }

    /// Returns the source's estimated distinct-value count.
    #[must_use]
    pub fn ndv(&self) -> i64 {
        (self.mask.wrapping_add(1) as i64).wrapping_mul(self.hashes.len() as i64)
    }

    /// Returns the source's portable sketch memory estimate.
    #[must_use]
    pub fn memory_usage(&self) -> u64 {
        16_u64.wrapping_add(8_u64.wrapping_mul(self.hashes.len() as u64))
    }

    /// Inserts an already-hashed value using the source level transition.
    pub fn insert_hash(&mut self, hash: u64) {
        // Go's `(hashVal & mask) != 0` is the complete admission rule.
        if hash & self.mask != 0 {
            return;
        }

        self.hashes.insert(hash);
        if self.hashes.len() > self.max_size {
            self.mask = self.mask.wrapping_mul(2).wrapping_add(1);
            let mask = self.mask;
            self.hashes.retain(|candidate| candidate & mask == 0);
        }
    }

    /// Inserts several already-hashed values in source stream order.
    pub fn insert_hashes<I>(&mut self, hashes: I)
    where
        I: IntoIterator<Item = u64>,
    {
        for hash in hashes {
            self.insert_hash(hash);
        }
    }

    /// Merges another sketch into this one.
    ///
    /// Go first raises the destination mask to the source mask and filters
    /// existing values, then replays source hashes through normal insertion.
    /// There is no dimension check: different `max_size` values are valid and
    /// the destination's threshold controls subsequent transitions.
    pub fn merge(&mut self, source: &Self) {
        if self.mask < source.mask {
            self.mask = source.mask;
            let mask = self.mask;
            self.hashes.retain(|candidate| candidate & mask == 0);
        }
        for &hash in &source.hashes {
            self.insert_hash(hash);
        }
    }
}
