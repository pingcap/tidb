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

//! Complete transcreation of Go `pkg/util/paging` (`paging.go`).
//!
//! A paging request may be split into multiple requests when there is more data
//! than a page. The paging size grows from a minimum to a maximum. See
//! <https://github.com/pingcap/tidb/issues/36328>.
//!
//! `main_test.go` is a goroutine-leak `TestMain` with no observable behavior of
//! its own; it has no Rust equivalent.

/// The minimum paging size.
pub const MIN_PAGING_SIZE: u64 = 128;
const MAX_PAGING_SIZE_SHIFT: u32 = 7;
const PAGING_SIZE_GROW: u64 = 2;
/// The minimum allowed value for the maximum paging size.
pub const MIN_ALLOWED_MAX_PAGING_SIZE: u64 = 50000;
const PAGING_GROWING_SUM: u64 = ((2u64 << MAX_PAGING_SIZE_SHIFT) - 1) * MIN_PAGING_SIZE;
/// The paging-size threshold.
pub const THRESHOLD: u64 = 960;

/// Grows the paging size and ensures it does not exceed
/// `max(maxv, MIN_ALLOWED_MAX_PAGING_SIZE)`.
#[must_use]
pub fn grow_paging_size(size: u64, mut maxv: u64) -> u64 {
    if maxv < MIN_ALLOWED_MAX_PAGING_SIZE {
        // Defensive programming, for example, called with maxv = 0. `maxv`
        // should never be less than `MIN_ALLOWED_MAX_PAGING_SIZE`; otherwise the
        // session variable may be wrong, or the distsql request does not obey
        // the session variable setting.
        maxv = MIN_ALLOWED_MAX_PAGING_SIZE;
    }

    let size = size << 1;
    if size > maxv {
        return maxv;
    }
    size
}

/// Calculates the seek count from the expected count.
#[must_use]
pub fn calculate_seek_cnt(expect_cnt: u64) -> f64 {
    if expect_cnt == 0 {
        return 0.0;
    }
    if expect_cnt > PAGING_GROWING_SUM {
        // If `expect_cnt` is larger than `PAGING_GROWING_SUM`, calculate the
        // seek count for the excess.
        return (8 + (expect_cnt - PAGING_GROWING_SUM).div_ceil(MIN_ALLOWED_MAX_PAGING_SIZE))
            as f64;
    }
    if expect_cnt > MIN_PAGING_SIZE {
        // If `expect_cnt` is less than `PAGING_GROWING_SUM`, calculate the seek
        // count (number of terms) from the sum of a geometric progression.
        //   expect_cnt = MIN_PAGING_SIZE * (PAGING_SIZE_GROW^seek_cnt - 1)
        //                                  / (PAGING_SIZE_GROW - 1)
        // Simplifying `PAGING_SIZE_GROW^seek_cnt - 1` to `PAGING_SIZE_GROW^seek_cnt`:
        //   seek_cnt = log((PAGING_SIZE_GROW - 1) * expect_cnt / MIN_PAGING_SIZE)
        //              / log(PAGING_SIZE_GROW)
        let ratio = (((PAGING_SIZE_GROW - 1) * expect_cnt) as f64 / MIN_PAGING_SIZE as f64).ln()
            / (PAGING_SIZE_GROW as f64).ln();
        return 1.0 + (ratio as i64) as f64;
    }
    1.0
}

#[cfg(test)]
mod tests {
    use super::{
        calculate_seek_cnt, grow_paging_size, MAX_PAGING_SIZE_SHIFT, MIN_ALLOWED_MAX_PAGING_SIZE,
        MIN_PAGING_SIZE, PAGING_GROWING_SUM, PAGING_SIZE_GROW,
    };

    fn assert_in_delta(actual: f64, expected: f64, delta: f64) {
        assert!(
            (actual - expected).abs() < delta,
            "expected {expected} +/- {delta}, got {actual}"
        );
    }

    // Go `TestGrowPagingSize`.
    #[test]
    fn grow_paging_size_test() {
        assert_eq!(
            grow_paging_size(MIN_PAGING_SIZE, MIN_ALLOWED_MAX_PAGING_SIZE),
            MIN_PAGING_SIZE * PAGING_SIZE_GROW
        );
        assert_eq!(
            grow_paging_size(MIN_ALLOWED_MAX_PAGING_SIZE, MIN_ALLOWED_MAX_PAGING_SIZE),
            MIN_ALLOWED_MAX_PAGING_SIZE
        );
        assert_eq!(
            grow_paging_size(
                MIN_ALLOWED_MAX_PAGING_SIZE / PAGING_SIZE_GROW + 1,
                MIN_ALLOWED_MAX_PAGING_SIZE
            ),
            MIN_ALLOWED_MAX_PAGING_SIZE
        );
    }

    // Go `TestCalculateSeekCnt`.
    #[test]
    fn calculate_seek_cnt_test() {
        let shift = f64::from(MAX_PAGING_SIZE_SHIFT);
        assert_in_delta(calculate_seek_cnt(0), 0.0, 0.1);
        assert_in_delta(calculate_seek_cnt(1), 1.0, 0.1);
        assert_in_delta(calculate_seek_cnt(MIN_PAGING_SIZE), 1.0, 0.1);
        assert_in_delta(calculate_seek_cnt(PAGING_GROWING_SUM), shift + 1.0, 0.1);
        assert_in_delta(calculate_seek_cnt(PAGING_GROWING_SUM + 1), shift + 2.0, 0.1);
        assert_in_delta(
            calculate_seek_cnt(PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE),
            shift + 2.0,
            0.1,
        );
    }
}
