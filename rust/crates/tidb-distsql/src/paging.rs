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

//! Row-paging growth and seek-count policy.
//!
//! This is the source-shaped policy leaf from `pkg/util/paging/paging.go`.
//! Request transport, range calculation, retry, and response handling remain
//! outside this module.

/// The first row-page size.
pub const MIN_PAGING_SIZE: u64 = 128;
const MAX_PAGING_SIZE_SHIFT: u64 = 7;
const PAGING_SIZE_GROW: u64 = 2;
/// The lower bound applied to a configured maximum row-page size.
pub const MIN_ALLOWED_MAX_PAGING_SIZE: u64 = 50_000;
const PAGING_GROWING_SUM: u64 = ((2 << MAX_PAGING_SIZE_SHIFT) - 1) * MIN_PAGING_SIZE;
/// The source threshold used by DistSQL paging decisions.
pub const PAGING_THRESHOLD: u64 = 960;

/// Doubles a page size, capped by the configured maximum after enforcing
/// [`MIN_ALLOWED_MAX_PAGING_SIZE`] as its lower bound.
#[must_use]
pub const fn grow_paging_size(size: u64, max: u64) -> u64 {
    let max = if max < MIN_ALLOWED_MAX_PAGING_SIZE {
        MIN_ALLOWED_MAX_PAGING_SIZE
    } else {
        max
    };

    // Go's uint64 left shift wraps modulo 2^64.
    let size = size.wrapping_shl(1);
    if size > max {
        max
    } else {
        size
    }
}

/// Calculates the number of paging seeks for an expected row count.
#[must_use]
pub fn calculate_seek_count(expected_count: u64) -> f64 {
    if expected_count == 0 {
        return 0.0;
    }
    if expected_count > PAGING_GROWING_SUM {
        // Preserve Go uint64 arithmetic, including wrapping at the upper edge.
        let excess = expected_count
            .wrapping_sub(PAGING_GROWING_SUM)
            .wrapping_add(MIN_ALLOWED_MAX_PAGING_SIZE)
            .wrapping_sub(1);
        return (8 + excess / MIN_ALLOWED_MAX_PAGING_SIZE) as f64;
    }
    if expected_count > MIN_PAGING_SIZE {
        let ratio = ((PAGING_SIZE_GROW - 1) * expected_count) as f64 / MIN_PAGING_SIZE as f64;
        return 1.0 + (ratio.ln() / (PAGING_SIZE_GROW as f64).ln()) as i64 as f64;
    }
    1.0
}
