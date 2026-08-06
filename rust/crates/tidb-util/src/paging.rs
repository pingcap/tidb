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

//! Lockdown owner for `pkg/util/paging/paging.go`.
//!
//! `paging.inventory.tsv` classifies every declaration, function, branch, and
//! arithmetic rule in that Go file. The source fingerprint and Rust symbol
//! gate below make an unreviewed source or inventory drift fail.

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

    let size = size.wrapping_shl(1);
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
        let rounded_excess = expect_cnt
            .wrapping_sub(PAGING_GROWING_SUM)
            .wrapping_add(MIN_ALLOWED_MAX_PAGING_SIZE - 1)
            / MIN_ALLOWED_MAX_PAGING_SIZE;
        return 8u64.wrapping_add(rounded_excess) as f64;
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
    use std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write as _,
    };

    use sha2::{Digest, Sha256};

    use super::{
        calculate_seek_cnt, grow_paging_size, MAX_PAGING_SIZE_SHIFT, MIN_ALLOWED_MAX_PAGING_SIZE,
        MIN_PAGING_SIZE, PAGING_GROWING_SUM, PAGING_SIZE_GROW, THRESHOLD,
    };

    const GO_SOURCE: &[u8] = include_bytes!("../../../../pkg/util/paging/paging.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("paging.inventory.tsv");
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 21] = [
        ("D01", ("PORTED", "MIN_PAGING_SIZE")),
        ("D02", ("PORTED", "MAX_PAGING_SIZE_SHIFT")),
        ("D03", ("PORTED", "PAGING_SIZE_GROW")),
        ("D04", ("PORTED", "MIN_ALLOWED_MAX_PAGING_SIZE")),
        ("D05", ("PORTED", "PAGING_GROWING_SUM")),
        ("D06", ("PORTED", "THRESHOLD")),
        ("F01", ("PORTED", "grow_paging_size")),
        ("B01", ("PORTED", "grow_paging_size")),
        ("B02", ("PORTED", "grow_paging_size")),
        ("R01", ("PORTED", "grow_paging_size")),
        ("B03", ("PORTED", "grow_paging_size")),
        ("B04", ("PORTED", "grow_paging_size")),
        ("F02", ("PORTED", "calculate_seek_cnt")),
        ("B05", ("PORTED", "calculate_seek_cnt")),
        ("B06", ("PORTED", "calculate_seek_cnt")),
        ("R02", ("PORTED", "calculate_seek_cnt")),
        ("R03", ("PORTED", "calculate_seek_cnt")),
        ("B07", ("PORTED", "calculate_seek_cnt")),
        ("R04", ("PORTED", "calculate_seek_cnt")),
        ("R05", ("PORTED", "calculate_seek_cnt")),
        ("B08", ("PORTED", "calculate_seek_cnt")),
    ];

    fn assert_in_delta(actual: f64, expected: f64, delta: f64) {
        assert!(
            (actual - expected).abs() < delta,
            "expected {expected} +/- {delta}, got {actual}"
        );
    }

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let recorded_hash = LOCKDOWN_INVENTORY
            .lines()
            .find_map(|line| line.strip_prefix("# source-sha256\t"))
            .expect("inventory records the owning Go source SHA-256");
        assert_eq!(recorded_hash, sha256_hex(GO_SOURCE), "Go source drifted");

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("id\tcategory\tgo_item\tstatus\trust_symbol\tevidence")
        );

        let allowed_statuses = BTreeSet::from(["PORTED", "DECLINED", "UNREACHABLE"]);
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 6, "invalid inventory row: {line}");
            assert!(
                allowed_statuses.contains(columns[3]),
                "unclassified inventory row: {line}"
            );
            assert!(
                !columns[5].is_empty(),
                "inventory evidence is required: {line}"
            );
            assert!(
                actual
                    .insert(columns[0], (columns[3], columns[4]))
                    .is_none(),
                "duplicate inventory id: {}",
                columns[0]
            );
        }
        assert_eq!(actual, BTreeMap::from(EXPECTED_ITEMS));

        assert_eq!(MIN_PAGING_SIZE, 128);
        assert_eq!(MAX_PAGING_SIZE_SHIFT, 7);
        assert_eq!(PAGING_SIZE_GROW, 2);
        assert_eq!(MIN_ALLOWED_MAX_PAGING_SIZE, 50_000);
        assert_eq!(PAGING_GROWING_SUM, 32_640);
        assert_eq!(THRESHOLD, 960);
        let _: fn(u64, u64) -> u64 = grow_paging_size;
        let _: fn(u64) -> f64 = calculate_seek_cnt;
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

    #[test]
    fn grow_paging_size_preserves_source_wrapping_and_cap_order() {
        assert_eq!(grow_paging_size(0, 0), 0);
        assert_eq!(grow_paging_size(25_000, 49_999), 50_000);
        assert_eq!(grow_paging_size(25_001, u64::MAX), 50_002);
        assert_eq!(grow_paging_size(u64::MAX / 2, u64::MAX), u64::MAX - 1);
        assert_eq!(grow_paging_size(u64::MAX / 2 + 1, u64::MAX), 0);
        assert_eq!(grow_paging_size(u64::MAX, u64::MAX), u64::MAX - 1);
        assert_eq!(grow_paging_size(u64::MAX, 0), MIN_ALLOWED_MAX_PAGING_SIZE);
    }

    #[test]
    fn calculate_seek_cnt_preserves_source_piecewise_boundaries() {
        assert_eq!(
            calculate_seek_cnt(0).to_bits(),
            0.0_f64.to_bits(),
            "the source returns positive zero"
        );

        let cases = [
            (1, 1.0),
            (MIN_PAGING_SIZE - 1, 1.0),
            (MIN_PAGING_SIZE, 1.0),
            (MIN_PAGING_SIZE + 1, 1.0),
            (2 * MIN_PAGING_SIZE - 1, 1.0),
            (2 * MIN_PAGING_SIZE, 2.0),
            (2 * MIN_PAGING_SIZE + 1, 2.0),
            (PAGING_GROWING_SUM - 1, 8.0),
            (PAGING_GROWING_SUM, 8.0),
            (PAGING_GROWING_SUM + 1, 9.0),
            (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE - 1, 9.0),
            (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE, 9.0),
            (PAGING_GROWING_SUM + MIN_ALLOWED_MAX_PAGING_SIZE + 1, 10.0),
        ];

        for (expect_cnt, expected) in cases {
            assert_eq!(calculate_seek_cnt(expect_cnt), expected, "{expect_cnt}");
        }
    }

    #[test]
    fn calculate_seek_cnt_preserves_source_ceil_addition_wrap() {
        const CEIL_OVERFLOW_BIAS: u64 = MIN_ALLOWED_MAX_PAGING_SIZE - 1 - PAGING_GROWING_SUM;
        let last_before_wrap = u64::MAX - CEIL_OVERFLOW_BIAS;
        let first_after_wrap = last_before_wrap + 1;
        let last_before_wrap_result = (8 + u64::MAX / MIN_ALLOWED_MAX_PAGING_SIZE) as f64;

        assert_eq!(
            calculate_seek_cnt(last_before_wrap - 1),
            last_before_wrap_result
        );
        assert_eq!(
            calculate_seek_cnt(last_before_wrap),
            last_before_wrap_result
        );
        assert_eq!(calculate_seek_cnt(first_after_wrap), 8.0);
        assert_eq!(calculate_seek_cnt(u64::MAX), 8.0);
    }

    fn sha256_hex(input: &[u8]) -> String {
        Sha256::digest(input)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                write!(output, "{byte:02x}").expect("write to String");
                output
            })
    }
}
